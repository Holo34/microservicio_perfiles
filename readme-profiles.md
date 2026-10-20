# Microservicio de Perfiles (`chat profiles`)

Este microservicio forma parte del sistema de chat distribuido y es responsable de gestionar los perfiles de usuario, incluyendo información personal, estados, redes sociales, intereses, idiomas y gestión de avatares.

## Tecnologías y Dependencias

* **Lenguaje & Framework:** Python 3.11 + FastAPI
* **Base de Datos:** MongoDB (Persistencia de perfiles con índice único en `user_id`)
* **Message Broker:** RabbitMQ (Publicación de eventos de perfil y consumo de eventos de usuario)
* **Servidor ASGI:** Uvicorn

### Dependencias

* `fastapi>=0.115` — Framework web principal y gestión de APIs.
* `uvicorn>=0.30` — Servidor ASGI para ejecutar la aplicación FastAPI.
* `pika>=1.3.0` — Cliente AMQP para comunicación asíncrona con RabbitMQ.
* `pymongo>=4.6` — Driver oficial para operaciones y persistencia en MongoDB.
* `python-multipart>=0.0.9` — Soporte para el procesamiento de carga de archivos (`UploadFile` para avatares).

## Versionamiento de la API

La API se encuentra versionada mediante el prefijo URL:
`http://<HOST>:<PUERTO>/api/v1/`

Todas las operaciones de la API de perfiles siguen la especificación `v1` para garantizar compatibilidad hacia atrás ante futuras modificaciones.

## Esquemas de Datos

El microservicio utiliza modelos Pydantic para la validación estricta de las solicitudes y respuestas.

### 1. `SocialHandles`

Redes sociales asociadas al usuario.

```json
{
  "github": "string | null (max_length: 50)",
  "linkedin": "string | null (max_length: 50)",
  "twitter": "string | null (max_length: 50)"
}
```

### 2. `ProfileUpdate`

Esquema utilizado para la actualización parcial (`PATCH`) del perfil.

```json
{
  "bio": "string | null (max_length: 280)",
  "status": "string | null (max_length: 60)",
  "social_handles": "SocialHandles | null",
  "interests": "Array[string] | null (max 10 elementos, cada uno entre 1 y 30 caracteres)",
  "languages": "Array[string] | null (max 10 elementos, códigos ISO ej: 'es', 'en')"
}
```

### 3. `Profile`

Representación completa de la entidad Perfil retornada por la API.

```json
{
  "user_id": "string",
  "display_name": "string",
  "bio": "string",
  "status": "string",
  "avatar_url": "string | null",
  "social_handles": {
    "github": "string | null",
    "linkedin": "string | null",
    "twitter": "string | null"
  },
  "interests": ["string"],
  "languages": ["string"],
  "created_at": "string (ISO 8601 UTC)",
  "updated_at": "string (ISO 8601 UTC)"
}
```

## Manejo de Errores y Códigos de Estado HTTP

El microservicio incluye un manejo estructurado de excepciones tanto en capa de aplicación como de infraestructura:

| Código HTTP | Descripción | Causa / Contexto |
| :--- | :--- | :--- |
| **404 Not Found** | User not found / Profile has no avatar | El `user_id` consultado no existe en la plataforma o se intentó eliminar un avatar inexistente. |
| **413 Payload Too Large** | Image larger than 2 MB | El archivo subido excede el límite permitido de 2 MB (`MAX_AVATAR_BYTES`). |
| **415 Unsupported Media Type** | Image must be PNG, JPEG or WEBP | Formato de archivo de imagen no válido según la validación de bytes mágicos. |
| **422 Unprocessable Entity** | Invalid or empty body | Error de validación Pydantic o petición `PATCH` enviada sin campos a actualizar. |
| **502 Bad Gateway** | Users service failed | El microservicio dependiente de `users` retornó un error al consultar un usuario. |
| **503 Service Unavailable** | Database unavailable / Users service unavailable | MongoDB está caído/inaccesible o no hay conectividad con el microservicio `users`. |

## Documentación de Endpoints

### Estado del Servicio

* `GET /health`
  * **Respuesta:** `{"status": "ok"}` (200 OK)

### Perfiles

* `GET /api/v1/profiles/{user_id}`
  * **Descripción:** Obtiene la vista pública de un perfil. Si el perfil no existe pero el usuario existe en `users`, genera el perfil predeterminado de forma transparente.
  * **Respuestas:** `200 OK`, `404 Not Found`, `502 Bad Gateway`, `503 Service Unavailable`.

* `PATCH /api/v1/profiles/{user_id}`
  * **Descripción:** Actualización parcial del perfil. Mantiene los datos no especificados y fusiona las redes sociales.
  * **Respuestas:** `200 OK`, `404 Not Found`, `422 Unprocessable Entity`.

### Avatares

* `POST /api/v1/profiles/{user_id}/avatar`
* `PUT /api/v1/profiles/{user_id}/avatar`
  * **Descripción:** Sube o reemplaza la foto de perfil mediante `multipart/form-data` (campo `avatar`).
  * **Respuestas:** `200 OK`, `404 Not Found`, `413 Payload Too Large`, `415 Unsupported Media Type`.

* `DELETE /api/v1/profiles/{user_id}/avatar`
  * **Descripción:** Elimina la foto de perfil actual del usuario y remueve el archivo del servidor.
  * **Respuestas:** `200 OK`, `404 Not Found`.

* `GET /avatars/{filename}`
  * **Descripción:** Sirve los archivos estáticos de avatares desde el directorio configurado (`AVATARS_DIR`).

### Eventos Local

* `GET /api/v1/events`
  * **Descripción:** Retorna el registro en memoria de los eventos procesados/emitidos localmente.

## 🔄 Arquitectura Orientada a Eventos

El microservicio soporta integración híbrida por eventos:

1. **Broker (RabbitMQ - Recomendado):**
   * **Consumo:** Escucha la cola `profiles.user-created` vinculada al exchange `chat` con la routing key `chat.user.created` para crear automáticamente el perfil inicial cuando un usuario se registra.
   * **Publicación:** Publica en el exchange `profiles` (tipo `topic`) los siguientes eventos:
     * `chat.profile.created`
     * `chat.profile.updated`
     * `chat.profile.avatar_updated`
     * `chat.profile.avatar_deleted`

2. **HTTP Polling (Fallback / Redundancia):**
   * En paralelo, ejecuta un hilo en segundo plano que consulta periódicamente (`time.sleep(3)`) el endpoint `GET /api/v1/events` de `USERS_URL` para procesar nuevos usuarios en caso de que RabbitMQ no esté activo.

## Variables de Entorno

| Variable | Valor por Defecto | Descripción |
| :--- | :--- | :--- |
| `MONGODB_URL` | *(Requerido)* | Cadena de conexión URI a MongoDB (ej: `mongodb://mongo:27017`). |
| `USERS_URL` | `http://users:8000` | Base URL del microservicio de Usuarios. |
| `RABBITMQ_URL` | `None` | URL de conexión a RabbitMQ (ej: `amqp://guest:guest@rabbitmq:5672/`). |
| `PUBLIC_URL` | `http://localhost:8000` | URL pública base usada para construir las URLs absolutas de avatares. |
| `AVATARS_DIR` | `/tmp/avatars` | Ruta interna donde se guardan las imágenes subidas. |

## Despliegue con Docker y Docker Compose

### 1. Requisitos Previos

Tener instalados Docker y Docker Compose en la máquina host.

### 2. Estructura del Proyecto

```text
.
├── app/
│   └── main.py
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

### 3. Archivo `docker-compose.yml`

Puedes ejecutar toda la plataforma con el siguiente archivo:

```yaml
version: "3.8"

services:
  profiles:
    build: .
    container_name: chat_profiles
    ports:
      - "8000:8000"
    environment:
      - MONGODB_URL=mongodb://mongo:27017
      - USERS_URL=http://users:8000
      - RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
      - PUBLIC_URL=http://localhost:8000
      - AVATARS_DIR=/app/avatars
    volumes:
      - avatars_data:/app/avatars
    depends_on:
      - mongo
      - rabbitmq

  mongo:
    image: mongo:6.0
    container_name: chat_mongo
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

  rabbitmq:
    image: rabbitmq:3-management
    container_name: chat_rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"

  users:
    image: python:3.11-slim
    container_name: chat_users
    command: sh -c "echo 'Servicio users mock' && sleep infinity"
    ports:
      - "8001:8000"

volumes:
  mongo_data:
  avatars_data:
```

### 4. Comandos de Despliegue

* **Iniciar todos los servicios:**
  ```bash
  docker-compose up -d --build
  ```

* **Ver logs del microservicio de perfiles:**
  ```bash
  docker-compose logs -f profiles
  ```

* **Detener la infraestructura:**
  ```bash
  docker-compose down
  ```
## Video Demostración

Link del video: 

## Declaración de uso de IA


