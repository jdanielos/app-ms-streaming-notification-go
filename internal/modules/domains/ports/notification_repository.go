package ports

import (
	"context"

	"github.com/streamingNotifyHub/internal/modules/domains/entities/notification"
)

// NotificationRepositoryInterface es todo lo que el core necesita de la base de
// datos. El core no sabe que hay Postgres detras.
type NotificationRepositoryInterface interface {
	// FindCategory devuelve la categoria del catalogo. Si no existe o esta
	// inactiva, devuelve `found=false`: el mensaje se descarta en vez de
	// inventarse un tipo por defecto, porque una categoria desconocida significa
	// que un emisor esta publicando algo que este servicio no sabe tratar.
	FindCategory(ctx context.Context, code string) (notification.Category, bool, error)

	// FindUserSettings devuelve las preferencias del usuario. Si el usuario aun
	// no tiene fila en `user_notification_settings`, devuelve los valores por
	// defecto de la tabla en vez de un error: la ausencia de preferencias no es
	// un fallo, es un usuario que nunca las toco.
	FindUserSettings(ctx context.Context, userID string) (notification.UserSettings, error)

	// Save guarda la notificacion y sus entregas en una sola transaccion.
	//
	// Devuelve `inserted=false` cuando el evento ya estaba guardado. No es un
	// error: RabbitMQ entrega al menos una vez, asi que el duplicado es el
	// funcionamiento normal, no la excepcion. El llamador confirma el mensaje
	// igual.
	Save(ctx context.Context, item notification.Notification, deliveries []notification.Delivery) (inserted bool, err error)
	ListInbox(ctx context.Context, userID string, limit int, cursor string) (notification.InboxPage, error)
	UnreadCount(ctx context.Context, userID string) (int, error)
}
