# Despliegue — separación API / WebSocket en AWS

Guía de referencia para desplegar `electric-backend` (API, puerto 8080) y
`websocket-electric` (WS Hub, puerto 8081) en el mismo cluster ECS Fargate,
misma VPC y subnets privadas, detrás de un ALB con path-based routing.

> No es Terraform — es referencia. Adapta ARNs, subnets y security groups.

---

## 1. ECS Task Definitions

### electric-api-task (puerto 8080)

```json
{
  "family": "electric-api-task",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "arn:aws:iam::<ACCOUNT_ID>:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::<ACCOUNT_ID>:role/electric-api-task-role",
  "containerDefinitions": [
    {
      "name": "electric-api",
      "image": "<ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/electric-api:latest",
      "essential": true,
      "portMappings": [
        { "containerPort": 8080, "protocol": "tcp" }
      ],
      "environment": [
        { "name": "PORT", "value": "8080" },
        { "name": "NODE_ENV", "value": "production" },
        { "name": "MONGODB_DATABASE", "value": "electricautomaticchile" },
        { "name": "CORS_ORIGINS", "value": "https://electricautomaticchile.com,https://www.electricautomaticchile.com" },
        { "name": "AUTH_COOKIE_DOMAIN", "value": ".electricautomaticchile.com" },
        { "name": "REDIS_DB", "value": "0" },
        { "name": "REDIS_TLS", "value": "true" },
        { "name": "REQUIRE_REDIS", "value": "true" }
      ],
      "secrets": [
        { "name": "MONGODB_URI", "valueFrom": "arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:electric/MONGODB_URI" },
        { "name": "JWT_SECRET",  "valueFrom": "arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:electric/JWT_SECRET" },
        { "name": "REDIS_URL",   "valueFrom": "arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:electric/REDIS_URL" }
      ],
      "healthCheck": {
        "command": ["CMD-SHELL", "wget -qO- http://localhost:8080/health || exit 1"],
        "interval": 30, "timeout": 5, "retries": 3, "startPeriod": 20
      },
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/electric-api",
          "awslogs-region": "<REGION>",
          "awslogs-stream-prefix": "api"
        }
      }
    }
  ]
}
```

### electric-ws-task (puerto 8081)

```json
{
  "family": "electric-ws-task",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "arn:aws:iam::<ACCOUNT_ID>:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::<ACCOUNT_ID>:role/electric-ws-task-role",
  "containerDefinitions": [
    {
      "name": "electric-ws",
      "image": "<ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/electric-ws:latest",
      "essential": true,
      "portMappings": [
        { "containerPort": 8081, "protocol": "tcp" }
      ],
      "environment": [
        { "name": "PORT", "value": "8081" },
        { "name": "NODE_ENV", "value": "production" },
        { "name": "CORS_ORIGINS", "value": "https://electricautomaticchile.com,https://www.electricautomaticchile.com" },
        { "name": "REDIS_DB", "value": "0" },
        { "name": "REDIS_TLS", "value": "true" }
      ],
      "secrets": [
        { "name": "JWT_SECRET", "valueFrom": "arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:electric/JWT_SECRET" },
        { "name": "REDIS_URL",  "valueFrom": "arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:electric/REDIS_URL" }
      ],
      "healthCheck": {
        "command": ["CMD-SHELL", "wget -qO- http://localhost:8081/health || exit 1"],
        "interval": 30, "timeout": 5, "retries": 3, "startPeriod": 15
      },
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/electric-ws",
          "awslogs-region": "<REGION>",
          "awslogs-stream-prefix": "ws"
        }
      }
    }
  ]
}
```

> Nota: la imagen distroless no trae `wget`. Para el health check del contenedor
> tienes dos opciones: (a) usar un binario de healthcheck compilado en Go incluido
> en la imagen, o (b) **omitir el container health check** y confiar en el
> health check del Target Group del ALB (recomendado, ver abajo). El `/health`
> HTTP del servicio funciona igual para el ALB.

---

## 2. ALB — Path-based routing

Dos Target Groups (ambos `target-type: ip` por Fargate/awsvpc):

| Target Group        | Puerto | Health check |
|---------------------|--------|--------------|
| `tg-electric-api`   | 8080   | `/health`    |
| `tg-electric-ws`    | 8081   | `/health`    |

Reglas del listener HTTPS (443), por prioridad:

```
Prioridad 10:  IF path-pattern = ["/ws", "/ws/*"]   →  forward  tg-electric-ws
Default:       (todo lo demás)                       →  forward  tg-electric-api
```

AWS CLI (referencia):

```bash
# Regla WS (mayor prioridad)
aws elbv2 create-rule \
  --listener-arn <LISTENER_ARN> \
  --priority 10 \
  --conditions Field=path-pattern,Values='/ws','/ws/*' \
  --actions Type=forward,TargetGroupArn=<TG_WS_ARN>

# El default del listener apunta a la API
aws elbv2 modify-listener \
  --listener-arn <LISTENER_ARN> \
  --default-actions Type=forward,TargetGroupArn=<TG_API_ARN>
```

### WebSocket upgrade en el ALB

- El ALB (Application Load Balancer) **soporta WebSocket nativamente**: reenvía
  los headers `Upgrade`/`Connection` y mantiene la conexión.
- Sube el **idle timeout** del ALB para que no corte conexiones WS inactivas:
  ```bash
  aws elbv2 modify-load-balancer-attributes \
    --load-balancer-arn <ALB_ARN> \
    --attributes Key=idle_timeout.timeout_seconds,Value=300
  ```
  El heartbeat ping/pong del Hub (cada ~54s) mantiene viva la conexión por debajo
  de ese timeout.
- **Sin stickiness**: el broadcast se hace vía Redis Pub/Sub, así que no hace
  falta `stickiness.enabled`. Déjalo desactivado en `tg-electric-ws`.

### Frontend

Apuntar el cliente WS al mismo dominio (el ALB enruta por path):

```
NEXT_PUBLIC_WS_URL = wss://api.electricautomaticchile.com/ws/connect
```

---

## 3. Migración sin downtime (strangler fig)

El objetivo es separar el WS del monolito sin cortar el servicio. Como el WS es
best-effort (datos en tiempo real, no transaccionales), la migración es de bajo
riesgo.

1. **Preparar Redis.** Ya usas ElastiCache para los locks del scheduler. Asegura
   que ambos servicios (API y WS) tengan acceso de red al mismo cluster Redis y
   el mismo `REDIS_DB`. Pub/Sub no requiere configuración extra.

2. **Desplegar el WS Hub nuevo (sin tráfico todavía).** Crea el servicio ECS
   `electric-ws` y su Target Group, pero **aún no** agregues la regla del ALB.
   Verifica `/health` directamente contra la IP de la task o un TG temporal.

3. **Activar la publicación de eventos en la API.** Despliega la versión de la
   API que publica en Redis (este cambio). Mientras el WS viejo siga embebido,
   puedes correr ambos en paralelo: la API publica en Redis **y** —si quisieras
   una fase intermedia— mantiene el hub local. En esta separación el hub local
   ya se eliminó, así que este paso coincide con el despliegue de la API nueva.

4. **Conmutar el routing del ALB.** Agrega la regla `path-pattern /ws,/ws/*` →
   `tg-electric-ws` con prioridad alta. A partir de aquí, las **conexiones WS
   nuevas** van al servicio dedicado. Las conexiones viejas siguen vivas en la
   API hasta que el cliente reconecte.

5. **Drenar las conexiones viejas.** Los clientes WS reconectan solos (el front
   tiene reconexión automática). En minutos, todo el tráfico WS está en el
   servicio nuevo. Monitorea `connectedClients` en `/health` de ambos.

6. **Eliminar el WS del monolito.** Una vez que `electric-ws` recibe todo el
   tráfico, retira el código WebSocket de la API (ya hecho en este cambio) y
   redespliega. La API queda solo con REST + publicación de eventos.

7. **Ajustar escalado y costos.** Ahora puedes dimensionar cada servicio por
   separado: la API por CPU/RAM de request, el WS por número de conexiones
   concurrentes (típicamente tasks más pequeñas y baratas). Configura
   autoscaling independiente por Target Group / métrica.

### Rollback

Si algo falla en el paso 4, **quita la regla del ALB**: el `/ws` vuelve al
default (API). Como el contrato de eventos y el JWT son idénticos, el rollback
es inmediato y sin pérdida de datos.
