import json
import os
import threading
import time
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import Annotated
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import urlopen
from uuid import uuid4
 
import pika
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

USERS_URL = os.environ.get("USERS_URL", "http://users:8000")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL")  #Copiado del demo del profe. messages/app/main.py.
events: list[dict] = []

#Avatares: se guardan como archivos en disco (no es la BD) y se sirven en /avatars/<archivo>.
#PUBLIC_URL es la direccion con la que los otros servicios/clientes ven a profiles (para armar avatar_url).
AVATARS_DIR = os.environ.get("AVATARS_DIR", "/tmp/avatars")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "http://localhost:8000").rstrip("/")
MAX_AVATAR_BYTES = 2 * 1024 * 1024  #2 MB


#Almacenamiento en memoria por ahora, sin BD
#Con la base de datos se deben reescribir estas 3 funciones con
#MongoDB (acordarse de excluir "_id"). No cambiar el resto del archivo.
#Clave: Id del usuario, valor: Perfil completo.

# SIN BD -> profiles: dict[str, dict] = {}

database = MongoClient(os.environ["MONGODB_URL"]).chat_profiles
profiles = database.profiles

#lo que usa el profe: database = MongoClient(os.environ["MONGODB_URL"]).chat_messages

#---Funciones que se comunicacn con la BD-----

""" SIN bd
#Busca un perfil y devuelve none si no existe. Devuelve el perfil si existe.
def find_profile(user_id: str) -> dict | None:
    return profiles.get(user_id)

#Guarda el perfil si no existe. Devuelve True si lo creó..
def insert_profile(profile: dict) -> bool:
    return profiles.setdefault(profile["user_id"], profile) is profile

#Modificar perfil y devuelve el perfil actualizado. 
def save_changes(user_id: str, cambios: dict) -> dict:
    profiles[user_id].update(cambios)
    return profiles[user_id]
"""

"""CON BD"""
#Busca un perfil y devuelve none si no existe. Devuelve el perfil si existe.
def find_profile(user_id: str) -> dict | None:
    return profiles.find_one({"user_id": user_id}, {"_id": 0})

#Guarda el perfil si no existe. Devuelve True si lo creó..
def insert_profile(profile: dict) -> bool:
    try:
        profiles.insert_one(dict(profile))  #copia: insert_one le agrega "_id" al dict que recibe
        return True
    except DuplicateKeyError:  #ya existia (indice unico en user_id)
        return False

#Modificar perfil y devuelve el perfil actualizado. 
def save_changes(user_id: str, cambios: dict) -> dict:
    return profiles.find_one_and_update({"user_id": user_id}, {"$set": cambios},
                                        return_document=ReturnDocument.AFTER,
                                        projection={"_id": 0})


#---Funciones de archivos de avatar (disco local, NO es la BD)-----
#Mira los primeros bytes del archivo para saber si es una imagen valida (no se confia en el content-type
#que manda el cliente). Devuelve la extension o None si no es png/jpg/webp.
def detectar_formato(contenido: bytes) -> str | None:
    if contenido.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if contenido.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if contenido[:4] == b"RIFF" and contenido[8:12] == b"WEBP":
        return "webp"
    return None

#Guarda la imagen con un nombre aleatorio (asi cada avatar nuevo tiene url nueva y no hay problemas de cache)
#y devuelve la url publica.
def guardar_avatar(contenido: bytes, extension: str) -> str:
    nombre = f"{uuid4().hex}.{extension}"
    with open(os.path.join(AVATARS_DIR, nombre), "wb") as archivo:
        archivo.write(contenido)
    return f"{PUBLIC_URL}/avatars/{nombre}"

#Borra el archivo de un avatar anterior a partir de su url. Si ya no existe no pasa nada.
def borrar_avatar(avatar_url: str | None) -> None:
    if not avatar_url:
        return
    nombre = os.path.basename(urlparse(avatar_url).path)  #basename evita salirse de AVATARS_DIR
    try:
        os.remove(os.path.join(AVATARS_DIR, nombre))
    except OSError:
        pass


#ESQUEMAS
Interes = Annotated[str, StringConstraints(min_length=1, max_length=30)]
Idioma = Annotated[str, StringConstraints(min_length=2, max_length=10)]  #Ej: "es", "en"


class SocialHandles(BaseModel):
    model_config = ConfigDict(extra="forbid")

    github: str | None = Field(default=None, max_length=50)
    linkedin: str | None = Field(default=None, max_length=50)
    twitter: str | None = Field(default=None, max_length=50)


#El avatar NO se cambia por aqui, tiene sus propios endpoints (/avatar) para poder validar la imagen.
class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")  #Si un campo enviado no existe, dará error

    bio: str | None = Field(default=None, max_length=280)
    status: str | None = Field(default=None, max_length=60)
    social_handles: SocialHandles | None = None
    interests: list[Interes] | None = Field(default=None, max_length=10)
    languages: list[Idioma] | None = Field(default=None, max_length=10)


class Profile(BaseModel):
    user_id: str
    display_name: str
    bio: str
    status: str
    avatar_url: str | None
    social_handles: SocialHandles
    interests: list[str]
    languages: list[str]
    created_at: str
    updated_at: str


#EVENTOS emitidos: chat.profile.created / chat.profile.updated / chat.profile.avatar_updated / chat.profile.avatar_deleted
#Igual como lo hizo el profe en el demo. messages: se guardan en la lista (GET /api/v1/events)
#y si hay RABBITMQ_URL, también se publican al exchange "profiles".
def publish(event_type: str, subject_id: str, data: dict) -> None:
    #deepcopy: sin esto el evento guardado apunta al mismo dict del perfil y cambiaria solo cuando el perfil se actualiza
    event = {"id": str(uuid4()), "type": event_type, "subject_id": subject_id,
                "occurred_at": datetime.now(timezone.utc).isoformat(), "data": deepcopy(data)}
    events.append(event)
    if RABBITMQ_URL:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            params.socket_timeout = 2
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange="profiles", exchange_type="topic",
                                        durable=True)
            action = event_type.rsplit(".", 1)[1]
            channel.basic_publish(
                exchange="profiles", routing_key=f"profile.{subject_id}.{action}",
                body=json.dumps(event),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent))
            connection.close()
        except (pika.exceptions.AMQPError, OSError):
            pass  #el evento queda en la lista local


#EVENTOS consumidos(cuando se crea un usuario): chat.user.created (servicio users). Lee el evento y crea el perfil.
def crear_perfil_default(user_id: str, display_name: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    profile = {"user_id": user_id, "display_name": display_name, "bio": "",
                "status": "", "avatar_url": None,
                "social_handles": {"github": None, "linkedin": None, "twitter": None},
                "interests": [], "languages": [],
                "created_at": now, "updated_at": now}
    if insert_profile(profile):  #Creo el perfil, si ya existía no hace nada (idempotente)
        publish("chat.profile.created", user_id, profile)

#Para seleccionar y trabajar solo con el evento de usuario creado
def handle_event(event: dict) -> None:
    if not isinstance(event, dict):  #si llega basura no debe botar el hilo
        return
    data = event.get("data")
    if event.get("type") == "chat.user.created" and isinstance(data, dict):
        if "id" in data:
            crear_perfil_default(data["id"], data.get("display_name", ""))

#Leer los eventos desde el endpoint(Si no hay cola funcionando aun(rabbit), esta es la forma de leer los eventos)-> GET /api/v1/events de users (el contrato que publicó el profe).
#Son redundantes esta funcion con la de consumir broker -> Como es idempotenten la creación de perfil, no tiene consecuencias relevantes el tener las dos al mismo tiempo.
def poll_users_events() -> None:
    vistos = 0
    while True:
        try:
            with urlopen(f"{USERS_URL}/api/v1/events", timeout=3) as response:
                users_events = json.loads(response.read())
            if len(users_events) < vistos:  #users se reinició
                vistos = 0
            for event in users_events[vistos:]:#Para seleccionar solo eventos que no haya visto antes(para no repetir)
                handle_event(event)
            vistos = len(users_events)
        except Exception:
            pass  #users caído, se reintenta
        time.sleep(3)

#Leer el broker de rabbit para ver las publicaciones hechas. Cuando users publique a RabbitMQ, llegan por aquí.
def consumir_broker() -> None:
    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = connection.channel()
            channel.exchange_declare(exchange="chat", exchange_type="topic")
            channel.queue_declare(queue="profiles.user-created", durable=True)
            channel.queue_bind(exchange="chat", queue="profiles.user-created",
                                routing_key="chat.user.created")#Suscripción a solo mensajes con eiqueta de user.created

            def callback(ch, method, properties, body):
                try:
                    handle_event(json.loads(body))
                except Exception:
                    pass  #mensaje mal formado, se descarta (si el perfil no se creó, get_or_create lo crea al consultarlo)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue="profiles.user-created",
                                    on_message_callback=callback)
            channel.start_consuming()
        except (pika.exceptions.AMQPError, OSError):
            time.sleep(3)


#Indice unico en user_id, asi no pueden existir dos perfiles del mismo usuario. Espera a que Mongo este arriba.
def preparar_indice() -> None:
    while True:
        try:
            profiles.create_index("user_id", unique=True)
            return
        except PyMongoError:
            time.sleep(2)

#Funciona para determinar como arranca el servicio.
@asynccontextmanager
async def lifespan(_: FastAPI):
    preparar_indice()
    threading.Thread(target=poll_users_events, daemon=True).start()
    if RABBITMQ_URL:
        threading.Thread(target=consumir_broker, daemon=True).start()
    yield


app = FastAPI(title="Chat Profiles", version="1.0.0", lifespan=lifespan)

#Si MongoDB falla durante una peticion, se responde 503 en vez de un 500 sin detalle.
@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, error: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "Database unavailable"})

#Sirve las imagenes guardadas: GET /avatars/<archivo>
os.makedirs(AVATARS_DIR, exist_ok=True)
app.mount("/avatars", StaticFiles(directory=AVATARS_DIR), name="avatars")

#Consulta GET /api/v1/users/{id} de users para saber si existe un usuario.
def obtener_user(user_id: str) -> dict | None:
    try:
        with urlopen(f"{USERS_URL}/api/v1/users/{quote(user_id, safe='')}", timeout=2) as response:
            return json.loads(response.read())
    except HTTPError as error:
        if error.code == 404:
            return None
        raise HTTPException(status_code=502, detail="Users service failed")
    except (URLError, TimeoutError):
        raise HTTPException(status_code=503, detail="Users service unavailable")

#Devuelve el perfil, si no existe pero el usuario si, lo crea.
def get_or_create(user_id: str) -> dict:
    profile = find_profile(user_id)#Busca el perfil
    if profile is None:#Si no tengo perfil
        user = obtener_user(user_id)#Veo si existe el usuario pero no el perfil
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")#No existe el user
        crear_perfil_default(user_id, user.get("display_name", ""))#Existe el user pero no el perfil, creo perfil
        profile = find_profile(user_id)
    return profile


#RUTAS
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/profiles/{user_id}", response_model=Profile,
        summary="Vista pública de un perfil",
        responses={404: {"description": "User not found"},
                    502: {"description": "Users service failed"},
                    503: {"description": "Users service unavailable"}})
def get_profile(user_id: str):
    return get_or_create(user_id)


#PATCH porque solo se modifican los campos que se envian, el resto queda igual.
@app.patch("/api/v1/profiles/{user_id}", response_model=Profile,
        summary="Actualiza bio, estado, redes sociales, intereses e idiomas",
        responses={404: {"description": "User not found"},
                    422: {"description": "Invalid or empty body"}})
def update_profile(user_id: str, body: ProfileUpdate):
    cambios = {}
    for k, v in body.model_dump(exclude_unset=True).items():
        if v is not None:
            cambios[k] = v
    if not cambios:
        raise HTTPException(status_code=422, detail="Send at least one field")
    profile = get_or_create(user_id)  #404 si el usuario no existe
    if "social_handles" in cambios:  #Solo cambia las redes enviadas, las demas se mantienen (null borra una red)
        cambios["social_handles"] = {**profile["social_handles"], **cambios["social_handles"]}
    cambios["updated_at"] = datetime.now(timezone.utc).isoformat()
    profile = save_changes(user_id, cambios)
    publish("chat.profile.updated", user_id, profile)
    return profile


#Foto de perfil: multipart/form-data con el archivo en el campo "avatar". POST y PUT hacen lo mismo.
ERRORES_AVATAR = {404: {"description": "User not found"},
                    413: {"description": "Image larger than 2 MB"},
                    415: {"description": "Image must be PNG, JPEG or WEBP"}}


#Modificar el avatar.
@app.post("/api/v1/profiles/{user_id}/avatar", response_model=Profile,
        summary="Sube o cambia la foto de perfil", responses=ERRORES_AVATAR)
@app.put("/api/v1/profiles/{user_id}/avatar", response_model=Profile,
        summary="Sube o cambia la foto de perfil", responses=ERRORES_AVATAR)
def upload_avatar(user_id: str, avatar: UploadFile = File(...)):
    profile = get_or_create(user_id)  #404 si el usuario no existe
    contenido = avatar.file.read(MAX_AVATAR_BYTES + 1)  #Lee 1 byte de mas para saber si se pasó del limite
    if len(contenido) > MAX_AVATAR_BYTES:
        raise HTTPException(status_code=413, detail="Image larger than 2 MB")
    extension = detectar_formato(contenido)
    if extension is None:
        raise HTTPException(status_code=415, detail="Image must be PNG, JPEG or WEBP")
    url_anterior = profile["avatar_url"]
    nueva_url = guardar_avatar(contenido, extension)
    try:
        profile = save_changes(user_id, {"avatar_url": nueva_url,
                                            "updated_at": datetime.now(timezone.utc).isoformat()})
    except Exception:
        borrar_avatar(nueva_url)  #Si la BD falla, no queda un archivo huerfano y el avatar anterior sigue intacto
        raise
    borrar_avatar(url_anterior)  #Recien con la BD actualizada se borra el archivo viejo
    publish("chat.profile.avatar_updated", user_id, profile)
    return profile


@app.delete("/api/v1/profiles/{user_id}/avatar", response_model=Profile,
        summary="Elimina la foto de perfil",
        responses={404: {"description": "User not found or profile has no avatar"}})
def delete_avatar(user_id: str):
    profile = get_or_create(user_id)  #404 si el usuario no existe
    if profile["avatar_url"] is None:
        raise HTTPException(status_code=404, detail="Profile has no avatar")
    borrar_avatar(profile["avatar_url"])
    profile = save_changes(user_id, {"avatar_url": None,
                                        "updated_at": datetime.now(timezone.utc).isoformat()})
    publish("chat.profile.avatar_deleted", user_id, profile)
    return profile


@app.get("/api/v1/events")
def list_events():
    return events
