# SPEC

## Objetivo

Convertir `notify-hub` en un hub de notificaciones real: consume los eventos que publican
los demas microservicios, los clasifica por categoria, decide a que canales van segun las
preferencias del usuario, los persiste y los entrega.

Hoy el servicio solo sabe mandar correos de OTP.

## Agente responsable

`notify-agent`

## Contexto

### Lo que ya existe

- **RabbitMQ configurado.** Exchange de tipo `topic` durable, cola durable, `QueueBind` con
  routing key `#`, consumo manual (`autoAck=false`) y 5 workers concurrentes.
- **Un consumidor**, `AuthEventConsumer`, atado a un unico DTO (`AuthEventDTO`) con forma de
  evento de autenticacion: correo, codigo, huella, asunto.
- **Una plantilla** de correo, `SendOtpsEmail.html`, cargada desde tres sitios.
- **`user_notification_settings`** en `ecosystem_core_auth`, con cuatro booleanos de
  preferencia.

### Lo que NO existe, y hay que decidir antes de empezar

**1. MQTT no esta configurado.** Un `grep` de `mqtt` sobre todo el repositorio no devuelve
nada; `go.mod` solo trae `amqp091-go`. Lo que hay es AMQP 0-9-1, que es otro protocolo.

Son dos cosas distintas y las dos hacen falta:

- **AMQP (RabbitMQ)** es el bus *entre microservicios*. Ya esta y sirve.
- **MQTT** seria el transporte *hacia el navegador*, para que la campana se actualice sin
  recargar.

Pero para eso el cliente **ya tiene un WebSocket montado**: `SCHEMA_GRAPHQL_WS.multimedia`,
con su `multimediaWsLink` en el router de Apollo. Meter MQTT sobre WebSocket seria un segundo
transporte de tiempo real en el mismo navegador, con su propia reconexion, su propia
autenticacion y su propio estado de conexion.

> **Decision: no se usa MQTT.**
>
> El motivo que decide no es el numero de transportes, es la **autorizacion**. Con el formato
> `mqtt://topic/message/{user_id}/{categoria}`, si el navegador se conecta directo al broker
> hace falta una ACL por topic para que un usuario no pueda suscribirse a `#` —o al topic de
> otro— y leer las notificaciones de todo el mundo. El plugin `rabbitmq_mqtt` autoriza a nivel
> de vhost y exchange; la ACL por usuario y topic exige un backend de autenticacion propio.
> Es trabajo real, y equivocarse ahi expone la bandeja de todos.
>
> A eso se suma que el WebSocket que ya existe en el cliente (`SCHEMA_GRAPHQL_WS`) apunta al
> puerto **3001**, y este servicio corre en el **3002**: reutilizarlo obliga a un salto de
> todas formas.
>
> El formato de direccion que definiste **se conserva igual** en la columna `topic`, como
> identificador del canal logico. Sirve para trazar y para reproducir, sin obligar a hablar
> MQTT de verdad.

### Entrega al navegador: WebSocket propio del hub

El hub abre **su propio WebSocket** en el 3002 y el navegador se conecta directo. No se pasa
por el gateway del 3001.

Es la decision correcta para esta arquitectura: hacer que el hub publique de vuelta a RabbitMQ
para que otro servicio lo repita crea una dependencia de despliegue —el 3001 tendria que
conocer el contrato de notificaciones y quedarse sin poder desplegarse por separado— y añade
un salto por el broker para nada.

El coste es que, de momento, el navegador mantiene dos sockets: uno al 3001 para los
comentarios y otro al 3002 para las notificaciones.

### Rumbo: un solo WebSocket

Esos dos sockets son un estado transitorio. La direccion es que **este servicio sea el unico
canal de tiempo real de la plataforma** y que la suscripcion de comentarios que hoy vive en el
3001 (`VideoCommentEvents`) acabe migrando aqui. Los demas microservicios dejarian de servir
WebSockets y solo publicarian al bus.

No se hace ahora, pero condiciona una decision de ahora: **el sobre del mensaje del socket no
puede ser especifico de notificaciones**. Si el primer mensaje que sale del socket es
`{ "type": "next", "payload": { ...notificacion } }` a secas, migrar los comentarios obliga a
romper el formato o a inventar un segundo canal dentro del mismo socket.

Por eso el sobre lleva `stream` desde el primer dia:

```json
{ "type": "next", "stream": "notifications", "payload": { } }
{ "type": "next", "stream": "video_comments", "payload": { } }   // cuando migre
```

Y el cliente se suscribe a flujos, no al socket entero:

```json
{ "type": "subscribe", "stream": "video_comments", "variables": { "videoId": "..." } }
```

Un campo de mas hoy; una migracion sin ruptura mañana.

```
  micro (multimedia/users)
        │ publish  notify.{cat}.{evento}
        ▼
   RabbitMQ exchange
        │
        ▼
   notify-hub (3002)
        │  valida · resuelve categoria · lee preferencias · persiste
        │
        ├──> WebSocket propio  ──────────────> navegador
        └──> worker de entrega (email / push)
```

**Handshake.** Se copia el que el cliente ya implementa para GraphQL, para no escribir un
segundo protocolo en el navegador:

```
cliente → { "type": "connection_init", "payload": { "headers": { ...auth } } }
servidor → { "type": "connection_ack" }
servidor → { "type": "next", "payload": { ...notificacion } }
```

**Autenticacion.** El usuario sale de las cabeceras del `connection_init`, nunca de un
parametro. El cliente ya manda `x-fingerprint`, `x-timestamp`, `x-signature` y `x-account-id`
via `getAuthenticatedHeaders()`.

> **Falta resolver como valida el hub esa firma.** El servicio no tiene libreria JWT y
> `adapters/out/services/authsOpts/` esta vacia. Pero **si tiene Redis** (`go-redis/v8`, ya en
> `go.mod`, y `REDIS_KEYS` declarado). Si el servicio de usuarios guarda ahi la sesion, el hub
> valida consultando Redis y no necesita compartir claves de firma con nadie. Hay que
> confirmarlo con el equipo de `users` antes de implementar el socket.
>
> Sin esa validacion el socket no se despliega: una conexion que se cree el `x-account-id` que
> le manden entrega la bandeja de cualquiera.

**Antes del socket, la fase 1.** La campana pide la bandeja al abrirse y sondea el contador de
no leidas. Es media tarde de trabajo, deja el circuito completo funcionando con datos reales,
y permite construir el socket contra algo que ya se ve en pantalla en vez de a ciegas.

**2. No hay base de datos.** `internal/modules/adapters/out/database/` esta vacia y `go.mod`
no tiene ningun driver. Hay que añadir `pgx/v5` y el adaptador.

**3. La cola se ata con `#`.** Consume *todo* lo que pase por el exchange. Con un solo emisor
funciona; con cinco microservicios publicando categorias distintas, este servicio recibe
tambien lo que no le toca y tiene que descartarlo en codigo, gastando red y ciclos.

**4. El reintento es un bucle caliente.** En `auth_consumer.go`, cuando el servicio esta en
pausa hace `d.Nack(false, true)`: devuelve el mensaje **al principio** de la cola. Los 5
workers lo vuelven a coger de inmediato, y el `time.Sleep(5s)` solo frena al worker que ya lo
solto. Ademas:

- No hay **cuenta de intentos**: un mensaje con JSON invalido se reencola para siempre.
- No hay **dead letter queue**: no existe sitio donde dejar lo que no se pudo procesar.
- No hay **idempotencia**: RabbitMQ entrega *al menos una vez*, asi que un reintento tras un
  corte de red duplica la notificacion.

Estos cuatro puntos son lo que separa "funciona" de "robusto", que es lo que pide esta spec.

## Reglas de negocio

- **Un evento, una notificacion.** La pareja `(source_service, event_id)` es unica. Si llega
  dos veces, la segunda no crea nada.
- **La categoria manda sobre el canal.** Cada categoria declara por que canales puede salir.
  Una notificacion de "empezo un directo" no se manda por correo aunque el usuario lo tenga
  encendido: llega tarde y molesta.
- **Las preferencias del usuario se respetan, salvo en seguridad.** `is_mandatory` en la
  categoria ignora `user_notification_settings`. Quien apaga los avisos no puede quedarse sin
  enterarse de que le entraron a la cuenta.
- **La notificacion y su entrega son cosas distintas.** Si rebota el correo, la copia en la
  campana sigue siendo valida. Por eso el estado de entrega vive en su propia tabla, por
  canal.
- **Lo que no se puede procesar se aparta, no se reintenta sin fin.** Tres intentos y a la
  DLQ.
- **El payload original se guarda entero.** Reprocesar no puede depender de volver a pedirle
  el dato al servicio que lo emitio.

## Modelo de datos

DDL completo en `agents/specs/ddl/001_notifications.sql`. Tres tablas nuevas en
`ecosystem_core_auth`, ninguna modificacion sobre lo existente:

| Tabla | Que guarda |
| --- | --- |
| `notification_categories` | Catalogo: que categorias hay y por que canales pueden salir. |
| `notifications` | Un evento consumido y resuelto a un usuario. Incluye `topic` y `payload`. |
| `notification_deliveries` | Un intento de entrega por canal, con su `terminal` y su estado. |

`terminal` es la direccion concreta del envio —el topic, el correo, el token del
dispositivo— y se **congela** en el momento del envio. Si el usuario cambia de correo, el
historial tiene que seguir diciendo a donde se mando de verdad.

## Contrato del evento

Los microservicios publican al exchange con routing key `notify.{categoria}.{evento}`:

```json
{
  "event_id": "0f3c...",
  "source_service": "multimedia",
  "event_type": "video.published",
  "category_code": "video",
  "user_id": "3cbbde2e-...",
  "actor_user_id": "9a71...",
  "title": "TheOspina publico un video",
  "body": "Doctor Strange en el multiverso",
  "action_url": "/watch/M2NiYmRlMmU...",
  "expires_at": null,
  "payload": { }
}
```

`event_id` lo genera el emisor y debe ser estable entre reintentos: es lo unico que evita
duplicados.

## Fronteras del cambio

### Puede tocar

- `internal/infrastructure/brokers/` — bindings, DLQ, QoS.
- `internal/modules/adapters/in/events/` — consumidor y DTOs.
- `internal/modules/adapters/out/database/` — repositorio nuevo.
- `internal/modules/core/notifications/` — orquestacion.
- `internal/modules/domains/` — entidades y puertos.
- `go.mod` — añadir `pgx/v5`.

### No debe tocar

- El flujo de correo de OTP que ya funciona. Se integra como una categoria mas
  (`auth_security`), sin reescribirlo.
- `user_notification_settings`. Se lee, no se modifica.
- El esquema de `users`.

## Plan de implementacion

1. **DDL.** Ejecutar `001_notifications.sql`. Es independiente del codigo y no rompe nada.
2. **Persistencia.** `pgx/v5`, pool en `fx`, repositorio en `out/database` detras de un
   puerto nuevo `NotificationRepositoryInterface`.
3. **Robustez del consumo**, antes de añadir funcionalidad:
   - Cola de dead letter (`notify.dlq`) con su exchange.
   - `x-dead-letter-exchange` en la cola principal y `Nack(requeue=false)` al superar
     intentos, para que el mensaje caiga en la DLQ en vez de volver a la cola.
   - `channel.Qos(prefetch)` — hoy sin limite, los 5 workers se traen toda la cola a memoria.
   - Cabecera `x-death` para contar intentos.
4. **Contrato de evento.** DTO nuevo `NotificationEventDTO`, con validacion. El `AuthEventDTO`
   actual se mantiene mientras migran los emisores.
5. **Bindings por categoria.** Sustituir `#` por `notify.*.*`, y dejar la puerta abierta a
   colas separadas por categoria si alguna crece.
6. **Orquestacion.** Resolver categoria, leer preferencias, decidir canales, persistir la
   notificacion y una fila de entrega por canal. **Aqui termina el trabajo del consumidor:**
   confirma el mensaje a RabbitMQ y suelta.
7. **Entrega, en un worker aparte.** Lee `notification_deliveries` en estado `pending` y las
   envia. `in_app` no envia nada —ya esta persistida—, `email` reutiliza lo que existe,
   `push` y `mqtt` quedan declarados sin implementar.

   > **Por que separado y no dentro del consumidor.** Si el proveedor de correo esta caido y
   > el envio va dentro del consumidor, la unica salida es `Nack` y reprocesar el evento
   > entero: se vuelve a consultar la categoria, las preferencias y se reintenta el `INSERT`.
   > Peor aun, el reintento de RabbitMQ y el del proveedor compiten entre si sin saberlo.
   >
   > Con la separacion, el consumidor solo responde de "esto quedo guardado", que es una
   > operacion rapida y contra una sola base de datos. Lo que falla despues es un problema de
   > la fila de entrega, con su propio `attempts` y su propio ritmo.
8. **Lectura.** Endpoint de bandeja y de "marcar leidas", que es lo que espera el
   `NotificationsMenu` del cliente — ya construido y hoy alimentado con lista vacia.

## Validacion

> **Aviso de despliegue.** La cola principal pasa a declararse con
> `x-dead-letter-exchange`. Si ya existe sin ese argumento, RabbitMQ rechaza la declaracion
> con `PRECONDITION_FAILED` y el servicio no arranca. Hay que **borrar la cola una vez** —con
> la cola vacia— para que se vuelva a crear con la DLX. Es un paso manual, y solo el primero.

- `go build ./...` y `go vet ./...`.
- Publicar el mismo `event_id` dos veces: debe existir una sola fila.
- Publicar un JSON invalido: debe acabar en la DLQ tras 3 intentos, no reencolarse.
- Usuario con `push_notifications = false` y categoria no obligatoria: la entrega `push`
  queda en `skipped`, la `in_app` en `sent`.
- Usuario con todo apagado y categoria `auth_security`: se entrega igual.
- Tumbar Postgres con la cola llena: los mensajes no se pierden, se reencolan.

## Riesgos

- **Duplicados si un emisor no manda `event_id` estable.** Se mitiga con la unicidad en base
  de datos, pero un emisor que genere el id en cada reintento la esquiva. Hay que acordarlo
  con cada equipo.
- **Crecimiento de `notifications`.** Sin barrido, la tabla crece indefinidamente. Los indices
  parciales ayudan, pero hace falta una politica de retencion; queda fuera de esta spec.
- **La DLQ sin nadie mirandola es un agujero silencioso.** Necesita alerta o revision
  periodica.
- **Añadir MQTT dobla el tiempo real del navegador.** Es el motivo de la recomendacion de
  arriba.

## Commits

- `feat(db): esquema de notificaciones, categorias y entregas`
- `feat(broker): dead letter queue, prefetch y limite de reintentos`
- `feat(notify): consumo, clasificacion y persistencia de notificaciones`
