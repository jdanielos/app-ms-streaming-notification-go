package events

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
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
}

func NewNotificationEventConsumer(
	channel *amqp091.Channel,
	queueName *string,
	usecase *notifications.ProcessNotificationUseCase,
	debouncer *notifications.NotificationDebouncer,
) *NotificationEventConsumer {
	return &NotificationEventConsumer{
		channel:   channel,
		queueName: queueName,
		usecase:   usecase,
		debouncer: debouncer,
	}
}

func (c *NotificationEventConsumer) Start() {
	if c.channel == nil || c.queueName == nil {
		slog.Error("consumidor de notificaciones sin canal de RabbitMQ")
		return
	}

	// Limita cuantos mensajes sin confirmar se lleva este consumidor. Sin esto
	// RabbitMQ entrega la cola entera de golpe: los workers se la traen a
	// memoria y, si el proceso muere, todo eso vuelve a la cola a la vez.
	if err := c.channel.Qos(constants.RABBITMQ_PREFETCH, 0, false); err != nil {
		slog.Error("no se pudo fijar el prefetch", "error", err)
		return
	}

	messages, err := c.channel.Consume(*c.queueName, "", false, false, false, false, nil)
	if err != nil {
		slog.Error("no se pudo registrar el consumidor", "error", err)
		return
	}

	for worker := 0; worker < consumerWorkers; worker++ {
		go func(workerID int) {
			for message := range messages {
				c.handle(message, workerID)
			}
		}(worker)
	}

	slog.Info("consumidor de notificaciones activo",
		slog.Int("workers", consumerWorkers),
		slog.Int("prefetch", constants.RABBITMQ_PREFETCH),
	)
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
