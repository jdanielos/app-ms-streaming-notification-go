package events

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"github.com/streamingNotifyHub/internal/infrastructure/realtime"
)

type RealtimeCommentEventConsumer struct {
	channel *amqp091.Channel
	hub     *realtime.NotificationHub
}

func NewRealtimeCommentEventConsumer(channel *amqp091.Channel, hub *realtime.NotificationHub) *RealtimeCommentEventConsumer {
	return &RealtimeCommentEventConsumer{channel: channel, hub: hub}
}

func (c *RealtimeCommentEventConsumer) Start() {
	if c.channel == nil {
		slog.Error("realtime_consumer_without_rabbit_channel")
		return
	}
	if err := c.channel.ExchangeDeclare(constants.REALTIME_WEBSOCKET_EXCHANGE, "topic", true, false, false, false, nil); err != nil {
		slog.Error("realtime_exchange_declare_failed", "error", err)
		return
	}
	if _, err := c.channel.QueueDeclare(constants.REALTIME_WEBSOCKET_QUEUE, true, false, false, false, nil); err != nil {
		slog.Error("realtime_queue_declare_failed", "error", err)
		return
	}
	if err := c.channel.QueueBind(constants.REALTIME_WEBSOCKET_QUEUE, "video.*.comment.*", constants.REALTIME_WEBSOCKET_EXCHANGE, false, nil); err != nil {
		slog.Error("realtime_queue_bind_failed", "error", err)
		return
	}
	if err := c.channel.QueueBind(constants.REALTIME_WEBSOCKET_QUEUE, "creator.*.follow.*", constants.REALTIME_WEBSOCKET_EXCHANGE, false, nil); err != nil {
		slog.Error("realtime_creator_follow_queue_bind_failed", "error", err)
		return
	}
	messages, err := c.channel.Consume(constants.REALTIME_WEBSOCKET_QUEUE, "notify-hub-realtime-comments", false, false, false, false, nil)
	if err != nil {
		slog.Error("realtime_consumer_register_failed", "error", err)
		return
	}
	go func() {
		// El bucle de fuera existe porque el de dentro TERMINA.
		//
		// Cuando RabbitMQ cierra la conexion —se reinicia, se cae, o el servidor
		// cierra el canal— el canal de mensajes se cierra y el `range` se acaba.
		// Antes la goroutine simplemente volvia y el hub se quedaba corriendo sin
		// consumir nada, para siempre y sin decir una palabra: los websockets
		// seguian abiertos, "escribiendo" seguia funcionando —eso no pasa por
		// Rabbit— y los comentarios nuevos dejaban de llegar. Justo el fallo que
		// costo encontrar.
		for {
			consumirMensajes(c, messages)

			slog.Error("realtime_consumer_stream_closed", "queue", constants.REALTIME_WEBSOCKET_QUEUE)

			// Se reintenta suscribirse. Si lo que se cerro fue solo el canal, esto
			// lo recupera solo; si lo que murio es la conexion entera, cada intento
			// falla y lo deja escrito, que es mejor que el silencio de antes.
			var err error
			for espera := time.Second; ; {
				messages, err = c.channel.Consume(constants.REALTIME_WEBSOCKET_QUEUE, "notify-hub-realtime-comments", false, false, false, false, nil)
				if err == nil {
					slog.Info("realtime_consumer_resumed", "queue", constants.REALTIME_WEBSOCKET_QUEUE)
					break
				}
				slog.Error("realtime_consumer_resume_failed", "error", err, "retry_in", espera.String())
				time.Sleep(espera)
				// Espera creciente con techo: reintentar cada segundo contra un
				// Rabbit caido durante horas solo llena el log.
				if espera < 30*time.Second {
					espera *= 2
				}
			}
		}
	}()
	slog.Info("realtime_comment_consumer_started", "exchange", constants.REALTIME_WEBSOCKET_EXCHANGE, "queue", constants.REALTIME_WEBSOCKET_QUEUE)
}

/** Consume hasta que el flujo se cierre. */
func consumirMensajes(c *RealtimeCommentEventConsumer, messages <-chan amqp091.Delivery) {
	for message := range messages {
		if strings.HasPrefix(message.RoutingKey, "creator.") {
			var event realtime.CreatorFollowEvent
			if err := json.Unmarshal(message.Body, &event); err != nil {
				slog.Error("realtime_creator_follow_event_decode_failed", "error", err)
				_ = message.Nack(false, false)
				continue
			}
			c.hub.PublishCreatorFollow(event)
			_ = message.Ack(false)
			continue
		}

		var event realtime.CommentEvent
		if err := json.Unmarshal(message.Body, &event); err != nil {
			slog.Error("realtime_event_decode_failed", "error", err)
			_ = message.Nack(false, false)
			continue
		}
		c.hub.PublishComment(event)
		_ = message.Ack(false)
	}
}
