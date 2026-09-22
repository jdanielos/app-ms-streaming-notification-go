package events

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/streamingNotifyHub/internal/infrastructure/brokers"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	dto "github.com/streamingNotifyHub/internal/modules/adapters/in/events/Dto"
	"github.com/streamingNotifyHub/internal/modules/core/notifications"
)

const (
	consumerWorkers = 5
	// Techo del tiempo que puede tardar un mensaje. Sin el, una consulta colgada
	// deja al worker bloqueado para siempre y la cola se acumula en silencio.
	processTimeout = 15 * time.Second
)

// NotificationEventConsumer consume el contrato nuevo de notificaciones.
//
// Convive con `AuthEventConsumer`, que sigue atendiendo los correos de OTP
// mientras los emisores migran.
type NotificationEventConsumer struct {
	channel   *amqp091.Channel
	queueName *string
	usecase   *notifications.ProcessNotificationUseCase
	debouncer *notifications.NotificationDebouncer
	// Pide un canal NUEVO cuando el actual muere - ver la nota de
	// `ChannelFactory`. Antes esto no existia: si Rabbit se caia, los 5
	// workers simplemente terminaban su `range` y el consumidor se quedaba
	// en silencio para siempre, sin un solo log que lo avisara.
	newChannel brokers.ChannelFactory
}

func NewNotificationEventConsumer(
	channel *amqp091.Channel,
	queueName *string,
	newChannel brokers.ChannelFactory,
	usecase *notifications.ProcessNotificationUseCase,
	debouncer *notifications.NotificationDebouncer,
) *NotificationEventConsumer {
	return &NotificationEventConsumer{
		channel:    channel,
		queueName:  queueName,
		newChannel: newChannel,
		usecase:    usecase,
		debouncer:  debouncer,
	}
}

func (c *NotificationEventConsumer) Start() {
	if c.channel == nil || c.queueName == nil {
		slog.Error("consumidor de notificaciones sin canal de RabbitMQ")
		return
	}

	messages, err := c.consume(c.channel)
	if err != nil {
		slog.Error("no se pudo registrar el consumidor", "error", err)
		return
	}

	go func() {
		// Mismo motivo que `realtime_consumer.go`: el `range` de cada worker
		// TERMINA cuando RabbitMQ cierra la conexion, y reintentar sobre el
		// mismo canal ya muerto nunca se recupera - hace falta uno nuevo.
		for {
			c.runWorkers(messages)

			slog.Error("notification_consumer_stream_closed", "queue", *c.queueName)

			for espera := time.Second; ; {
				newCh, err := c.newChannel()
				if err == nil {
					messages, err = c.consume(newCh)
				}
				if err == nil {
					c.channel = newCh
					slog.Info("notification_consumer_resumed", "queue", *c.queueName)
					break
				}
				slog.Error("notification_consumer_resume_failed", "error", err, "retry_in", espera.String())
				time.Sleep(espera)
				if espera < 30*time.Second {
					espera *= 2
				}
			}
		}
	}()

	slog.Info("consumidor de notificaciones activo",
		slog.Int("workers", consumerWorkers),
		slog.Int("prefetch", constants.RABBITMQ_PREFETCH),
	)
}

// consume fija el prefetch (es por canal, no sobrevive a un canal nuevo) y
// registra el consumidor sobre `ch`.
func (c *NotificationEventConsumer) consume(ch *amqp091.Channel) (<-chan amqp091.Delivery, error) {
	// Limita cuantos mensajes sin confirmar se lleva este consumidor. Sin esto
	// RabbitMQ entrega la cola entera de golpe: los workers se la traen a
	// memoria y, si el proceso muere, todo eso vuelve a la cola a la vez.
	if err := ch.Qos(constants.RABBITMQ_PREFETCH, 0, false); err != nil {
		return nil, err
	}
	return ch.Consume(*c.queueName, "", false, false, false, false, nil)
}

// runWorkers levanta los workers y bloquea hasta que todos terminan -
// exactamente cuando `messages` se cierra por una conexion caida.
func (c *NotificationEventConsumer) runWorkers(messages <-chan amqp091.Delivery) {
	var wg sync.WaitGroup
	for worker := 0; worker < consumerWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for message := range messages {
				c.handle(message, workerID)
			}
		}(worker)
	}
	wg.Wait()
}

func (c *NotificationEventConsumer) handle(message amqp091.Delivery, workerID int) {
	log := slog.With(
		slog.Int("worker", workerID),
		slog.String("routing_key", message.RoutingKey),
	)

	var event dto.NotificationEventDTO
	if err := json.Unmarshal(message.Body, &event); err != nil {
		// Un JSON roto no se arregla reintentandolo.
		discard(message, log, "mensaje ilegible", err)
		return
	}

	if err := event.Validate(); err != nil {
		discard(message, log.With(slog.String("event_id", event.EventID)), "mensaje incompleto", err)
		return
	}

	if queued, err := c.debouncer.Enqueue(event, message.RoutingKey); err != nil {
		log.Warn("no se pudo agrupar la notificacion", "error", err)
		message.Nack(false, true)
		return
	} else if queued {
		message.Ack(false)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
	defer cancel()

	_, err := c.usecase.Execute(ctx, event, message.RoutingKey)

	// Una categoria que no existe es un problema del emisor, no algo temporal.
	// A la DLQ, para que alguien lo mire.
	if errors.Is(err, notifications.ErrUnknownCategory) {
		discard(message, log.With(
			slog.String("category", event.CategoryCode),
			slog.String("source", event.SourceService),
		), "categoria desconocida", err)
		return
	}

	if err != nil {
		// Aqui el fallo si puede ser pasajero —la base caida, por ejemplo—, asi
		// que se reintenta. Pero con tope: pasados los intentos, a la DLQ.
		attempts := deathCount(message)
		if attempts >= constants.RABBITMQ_MAX_ATTEMPTS {
			discard(message, log.With(slog.Int("attempts", attempts), slog.String("event_id", event.EventID)),
				"agotados los reintentos", err)
			return
		}

		log.Warn("fallo al procesar, se reintenta",
			"error", err,
			slog.Int("attempts", attempts),
			slog.String("event_id", event.EventID),
		)
		// `requeue=false` y no `true`: con `true` el mensaje vuelve al principio
		// de la cola y los workers lo recogen al instante, formando un bucle
		// caliente que no deja avanzar al resto. Al ir por la DLX, RabbitMQ
		// cuenta el intento en `x-death` y el mensaje vuelve por el camino
		// largo, dando tiempo a que lo que fallaba se recupere.
		message.Nack(false, false)
		return
	}

	message.Ack(false)
}

// discard aparta un mensaje que no se puede procesar, dejando SIEMPRE el cuerpo
// en el log antes de soltarlo.
//
// El registro no es opcional: `Nack(requeue=false)` manda el mensaje a la DLQ
// solo si la cola esta declarada con `x-dead-letter-exchange`. Si la cola es
// antigua y no lo tiene —el caso al actualizar un despliegue en marcha—, RabbitMQ
// lo BORRA sin mas. Con el cuerpo en el log, ese mensaje se puede recuperar y
// reinyectar a mano; sin el, desaparece y nadie se entera.
func discard(message amqp091.Delivery, log *slog.Logger, reason string, cause error) {
	log.Error("mensaje descartado: "+reason,
		"error", cause,
		slog.String("body", string(message.Body)),
	)
	message.Nack(false, false)
}

// deathCount lee cuantas veces paso ya este mensaje por la dead letter exchange.
// RabbitMQ mantiene la cuenta en la cabecera `x-death`; sin leerla no hay forma
// de distinguir el primer intento del decimo.
func deathCount(message amqp091.Delivery) int {
	deaths, ok := message.Headers["x-death"].([]any)
	if !ok || len(deaths) == 0 {
		return 0
	}

	entry, ok := deaths[0].(amqp091.Table)
	if !ok {
		return 0
	}

	switch count := entry["count"].(type) {
	case int64:
		return int(count)
	case int32:
		return int(count)
	case int:
		return count
	default:
		return 0
	}
}
