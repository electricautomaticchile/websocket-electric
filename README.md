# websocket-electric

Servicio WebSocket Hub independiente de Electric Automatic. Separado de
`electric-backend` (API REST) para escalar de forma independiente y reducir
costos en AWS.

## Arquitectura

```
                         Redis Pub/Sub (bus de eventos)
                          ┌──────────────────────────┐
                          │   ws:events / ws:broadcast │
                          └──────────────────────────┘
                            ▲ publica            │ suscribe
                            │                    ▼
   ┌──────────────────────────────┐   ┌──────────────────────────────┐
   │      electric-backend (API)  │   │   websocket-electric (Hub)    │
   │  REST · auth · CRUD · IoT    │   │  WS · broadcasts · alertas    │
   │  Publica eventos en Redis    │   │  Entrega a clientes WS        │
   └──────────────────────────────┘   └──────────────────────────────┘
            puerto 8080                          puerto 8081
                            ▲                    ▲
                            │   ALB path-based   │
                  /* ───────┘        /ws, /ws/* ─┘
```

- La **API no habla directamente con el WS**. Publica `Event` en Redis Pub/Sub.
- El **WS Hub** se suscribe a los canales `ws:events` y `ws:broadcast`, y entrega
  los mensajes a los clientes conectados (por cliente, por empresa o global).
- **Sin sticky sessions**: como cada instancia del WS recibe todos los eventos
  vía Pub/Sub, cualquier task puede atender a cualquier cliente. El ALB puede
  balancear libremente.
- **Autenticación**: el handshake WebSocket valida el mismo JWT (HS256) que la
  API, con la misma `JWT_SECRET`.

## Endpoints

| Método | Ruta           | Descripción                               |
|--------|----------------|-------------------------------------------|
| GET    | `/ws/connect`  | Upgrade a WebSocket (requiere JWT)        |
| GET    | `/ws`          | Alias de `/ws/connect`                    |
| GET    | `/health`      | Health check (JSON, para ALB/ECS)         |

El JWT se toma de (en orden): cookie `auth_token`, header `Authorization: Bearer`,
o query param `?token=`.

## Contrato de eventos (Redis)

La API publica un `Event` JSON en el canal correspondiente:

```json
{
  "scope": "cliente",            // "cliente" | "empresa" | "all"
  "targetId": "665f...",         // id del cliente/empresa (vacío si scope=all)
  "message": {
    "type": "device_update",     // alert | notification | device_update | consumption
    "data": { "...": "..." },
    "timestamp": "2026-06-27T20:00:00Z",
    "empresaId": "665f...",
    "clienteId": "665f..."
  }
}
```

- `scope: cliente` / `scope: empresa` → canal `ws:events`
- `scope: all` → canal `ws:broadcast`

## Variables de entorno

| Variable        | Requerida | Default                  | Descripción                                                        |
|-----------------|-----------|--------------------------|--------------------------------------------------------------------|
| `PORT`          | no        | `8081`                   | Puerto HTTP/WS del servicio (ALB enruta `/ws` aquí).               |
| `JWT_SECRET`    | **sí**    | —                        | Clave HS256. **Debe ser idéntica** a la de la API (mín. 32 chars). |
| `REDIS_URL`     | no        | —                        | URL completa `redis://` o `rediss://`. Sobrescribe los campos individuales. |
| `REDIS_HOST`    | no        | `localhost`              | Host de Redis/ElastiCache.                                         |
| `REDIS_PORT`    | no        | `6379`                   | Puerto de Redis.                                                   |
| `REDIS_PASSWORD`| no        | —                        | Password de Redis (si aplica).                                     |
| `REDIS_DB`      | no        | `0`                      | Número de base de datos Redis.                                     |
| `REDIS_TLS`     | no        | `false`                  | `true` para conexiones TLS (`rediss`). Auto-`true` si `REDIS_URL` usa `rediss://`. |
| `CORS_ORIGINS`  | no        | `http://localhost:3000`  | Orígenes permitidos en el handshake (separados por coma).          |
| `NODE_ENV`      | no        | `development`            | Entorno (`development`/`production`).                              |

## Desarrollo local

```bash
cp .env.example .env      # ajustar JWT_SECRET y REDIS_*
go run .
# WS Hub en http://localhost:8081  (ws://localhost:8081/ws/connect)
```

Con el workspace (`go.work` en la raíz del monorepo) ambos servicios comparten
resolución de módulos:

```bash
go build ./electric-backend/... ./websocket-electric/...
```

## Docker

```bash
docker build -t electric-ws:latest .
docker run --rm -p 8081:8081 \
  -e JWT_SECRET=... \
  -e REDIS_URL=rediss://:pass@mi-elasticache:6379 \
  -e CORS_ORIGINS=https://electricautomaticchile.com \
  electric-ws:latest
```
