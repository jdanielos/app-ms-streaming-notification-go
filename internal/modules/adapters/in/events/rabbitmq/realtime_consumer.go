package events

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/streamingNotifyHub/internal/infrastructure/brokers"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"github.com/streamingNotifyHub/internal/infrastructure/realtime"
)

type RealtimeCommentEventConsumer struct {
	channel *amqp091.Channel
	// Pide un canal NUEVO cuando el actual muere. Reintentar `Consume` sobre
	// el mismo `channel` de arriba nunca se recupera de una conexion caida -
	// ver la nota de `ChannelFactory`.
	newChannel brokers.ChannelFactory
	hub        *realtime.NotificationHub
}

func NewRealtimeCommentEventConsumer(channel *amqp091.Channel, newChannel brokers.ChannelFactory, hub *realtime.NotificationHub) *RealtimeCommentEventConsumer {
	return &RealtimeCommentEventConsumer{channel: channel, newChannel: newChannel, hub: hub}
}

// setupTopology declara el exchange, la cola y los dos bindings sobre el
// canal que se le pase. Un canal recien abierto no hereda nada de lo que se
// declaro en uno anterior - hace falta repetirlo cada vez que se reconecta,
// no solo la primera vez. Declarar algo que ya existe (con los mismos
// argumentos) no falla, asi que es seguro repetirlo.
func setupTopology(ch *amqp091.Channel) error {
	if err := ch.ExchangeDeclare(constants.REALTIME_WEBSOCKET_EXCHANGE, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(constants.REALTIME_WEBSOCKET_QUEUE, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(constants.REALTIME_WEBSOCKET_QUEUE, "video.*.comment.*", constants.REALTIME_WEBSOCKET_EXCHANGE, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(constants.REALTIME_WEBSOCKET_QUEUE, "video.*.like.*", constants.REALTIME_WEBSOCKET_EXCHANGE, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(constants.REALTIME_WEBSOCKET_QUEUE, "creator.*.follow.*", constants.REALTIME_WEBSOCKET_EXCHANGE, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(constants.REALTIME_WEBSOCKET_QUEUE, "live.*.chat.*", constants.REALTIME_WEBSOCKET_EXCHANGE, false, nil); err != nil {
		return err
	}
	return nil
}

func (c *RealtimeCommentEventConsumer) Start() {
	if c.channel == nil {
		slog.Error("realtime_consumer_without_rabbit_channel")
		return
	}
	if err := setupTopology(c.channel); err != nil {
		slog.Error("realtime_topology_setup_failed", "error", err)
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

			// Se pide un canal NUEVO, no se reintenta sobre el viejo: ese ya
			// esta muerto (fue lo que cerro el `range` de arriba) y volver a
			// llamar `Consume` en el nunca funciona, solo repite el mismo
			// error para siempre. `newChannel()` redialea la conexion si
			// hace falta.
			for espera := time.Second; ; {
				newCh, err := c.newChannel()
				if err == nil {
					if err = setupTopology(newCh); err == nil {
						messages, err = newCh.Consume(constants.REALTIME_WEBSOCKET_QUEUE, "notify-hub-realtime-comments", false, false, false, false, nil)
					}
				}
				if err == nil {
					c.channel = newCh
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
		if strings.HasPrefix(message.RoutingKey, "live.") {
			var event realtime.LiveChatEvent
			if err := json.Unmarshal(message.Body, &event); err != nil || event.SessionID == "" || len(event.Message) == 0 {
				slog.Error("realtime_live_chat_event_decode_failed", "error", err, "routing_key", message.RoutingKey)
				_ = message.Nack(false, false)
				continue
			}
			c.hub.PublishLiveChat(event)
			_ = message.Ack(false)
			continue
		}
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
