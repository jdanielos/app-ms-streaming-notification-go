package constants

const API_ROUTER_STABLE = "/api/" + "v1" + "/system" + "/nofify"

var REDIS_KEYS = [1]string{"auth:register_otp"}

const ENV_RABBITMQ_URL = "RABBITMQ_URL"

const ENV_CHANELRABBITMQ_NOTIFY_TOPIC = "CHANELRABBITMQ_NOTIFY_TOPIC"
const ENV_CHANELRABBITMQ_NOTIFY_RMSG = "CHANELRABBITMQ_NOTIFY_MSG"

const ENV_CREDENTIALS_EMAIL_PROVIDER = "CREDENTIALS_EMAIL_PROVIDER"
const ENV_CREDENTIALS_FROM_EMAIL_PROVIDER = "CREDENTIALS_FROM_EMAIL_PROVIDER"

// Base de datos de usuarios: ahi viven `users`, `user_notification_settings` y
// las tablas de notificaciones. El hub no tiene base propia a proposito: las
// notificaciones apuntan al usuario con una clave foranea real, y eso solo se
// puede garantizar dentro del mismo motor.
const ENV_DATABASE_USERS_URL = "DATABASE_USERS_URL"

// Cola de mensajes que no se pudieron procesar. Es el destino del descarte, no
// un reintento: lo que cae aqui se mira a mano.
const CHANELRABBITMQ_NOTIFY_DLX = "notify.dlx"
const CHANELRABBITMQ_NOTIFY_DLQ = "notify.dlq"

// Exchange y cola exclusivos del gateway realtime. No se mezclan con las
// notificaciones persistibles: los comentarios se entregan a los sockets y no
// pasan por el flujo de correo/DB.
const REALTIME_WEBSOCKET_EXCHANGE = "realtime.websocket.events"
const REALTIME_WEBSOCKET_QUEUE = "realtime_websocket_queue"

// Cuantos mensajes se lleva cada consumidor sin confirmar. Sin este limite
// RabbitMQ entrega toda la cola de golpe y los 5 workers se la traen entera a
// memoria; con el, nadie acumula mas trabajo del que puede terminar.
const RABBITMQ_PREFETCH = 10

// Intentos antes de mandar el mensaje a la DLQ. Un JSON invalido no mejora por
// reintentarlo, y sin tope se reencola para siempre.
const RABBITMQ_MAX_ATTEMPTS = 3
