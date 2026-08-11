package notifications

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	dto "github.com/streamingNotifyHub/internal/modules/adapters/in/events/Dto"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/notification"
	"github.com/streamingNotifyHub/internal/modules/domains/ports"
)

// ErrUnknownCategory indica que el emisor uso una categoria que no esta en el
// catalogo, o que esta desactivada. No se reintenta: reintentar no la va a
// crear. El consumidor lo trata como descarte.
var ErrUnknownCategory = errors.New("categoria desconocida o inactiva")

// ProcessNotificationUseCase es lo unico que ocurre al consumir un mensaje:
// resolver, decidir y guardar. La entrega la hace despues otro worker leyendo
// `notification_deliveries`.
//
// Esa separacion es deliberada. Si el envio viviera aqui, un proveedor de correo
// caido obligaria a devolver el mensaje a RabbitMQ y reprocesar el evento
// entero —volver a leer categoria, preferencias y reintentar el INSERT—, y el
// reintento del broker competiria con el del proveedor sin saberlo.
type ProcessNotificationUseCase struct {
	repository ports.NotificationRepositoryInterface
	publisher  ports.NotificationPublisher
}

func NewProcessNotificationUseCase(
	repository ports.NotificationRepositoryInterface,
	publisher ports.NotificationPublisher,
) *ProcessNotificationUseCase {
	return &ProcessNotificationUseCase{repository: repository, publisher: publisher}
}

// Execute devuelve `stored=false` cuando el evento ya estaba guardado. El
// llamador confirma el mensaje igual: un duplicado es el funcionamiento normal
// de una cola que entrega al menos una vez.
func (uc *ProcessNotificationUseCase) Execute(
	ctx context.Context,
	event dto.NotificationEventDTO,
	routingKey string,
) (stored bool, err error) {
	category, found, err := uc.repository.FindCategory(ctx, event.CategoryCode)
	if err != nil {
		return false, err
	}
	if !found {
		return false, ErrUnknownCategory
	}

	settings, err := uc.repository.FindUserSettings(ctx, event.UserID)
	if err != nil {
		return false, err
	}

	channels := notification.ResolveChannels(category, settings)

	item := notification.Notification{
		NotificationID: event.EventID,
		UserID:        event.UserID,
		CategoryCode:  event.CategoryCode,
		SourceService: event.SourceService,
		EventType:     event.EventType,
		EventID:       event.EventID,
		Topic:         notification.BuildTopic(topicBase(), event.UserID, event.CategoryCode),
		RoutingKey:    routingKey,
		Title:         event.Title,
		Body:          event.Body,
		ActionURL:     event.ActionURL,
		ActorUserID:   event.ActorUserID,
		Payload:       event.RawPayload(),
		Status:        notification.StatusProcessed,
		ExpiresAt:     event.ExpiresAt,
		CreatedAt:     time.Now().UTC(),
	}

	deliveries := make([]notification.Delivery, 0, len(channels))
	for channel, status := range channels {
		deliveries = append(deliveries, notification.Delivery{
			Channel:  channel,
			Terminal: terminalFor(channel, item.Topic, event.UserID),
			Status:   status,
		})
	}

	stored, err = uc.repository.Save(ctx, item, deliveries)
	if err != nil {
		return false, err
	}

	if !stored {
		slog.Debug("evento repetido, se ignora",
			slog.String("source", event.SourceService),
			slog.String("event_id", event.EventID),
		)
		return false, nil
	}

	slog.Info("notificacion guardada",
		slog.String("user_id", event.UserID),
		slog.String("category", event.CategoryCode),
		slog.Int("deliveries", len(deliveries)),
	)
	uc.publisher.Publish(item)

	return true, nil
}

// terminalFor resuelve la direccion concreta de cada canal.
//
// Correo y push quedan pendientes: el correo esta en `users` y el token del
// dispositivo aun no se guarda en ningun sitio. Se registra la entrega con la
// direccion vacia marcada, para que el worker sepa que le falta el dato en vez
// de fallar en el envio.
func terminalFor(channel, topic, userID string) string {
	switch channel {
	case notification.ChannelInApp, notification.ChannelMQTT:
		return topic
	default:
		// TODO(backend): resolver el correo desde `users` y el token de
		// dispositivo cuando exista la tabla.
		return "pending:" + userID
	}
}

// topicBase es el nombre del exchange, para que el topic guardado apunte al
// canal real por el que viajo el mensaje.
func topicBase() string {
	if base := os.Getenv(constants.ENV_CHANELRABBITMQ_NOTIFY_TOPIC); base != "" {
		return base
	}
	return "notify"
}
