package modules

import (
	"github.com/gofiber/contrib/websocket"
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"log/slog"

	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"github.com/streamingNotifyHub/internal/infrastructure/database"
	"github.com/streamingNotifyHub/internal/infrastructure/realtime"
	"github.com/streamingNotifyHub/internal/infrastructure/types"
	events "github.com/streamingNotifyHub/internal/modules/adapters/in/events/rabbitmq"
	handlers "github.com/streamingNotifyHub/internal/modules/adapters/in/handlers/notifications"
	outdb "github.com/streamingNotifyHub/internal/modules/adapters/out/database"
	"github.com/streamingNotifyHub/internal/modules/core/notifications"
	"github.com/streamingNotifyHub/internal/modules/domains/ports"
	"go.uber.org/fx"
)

func ConfigureNotificationWebsocket(
	hub *realtime.NotificationHub,
	inbox *handlers.InboxHandler,
	store *types.HandlersStore,
	_ *config.AppSettings,
) {
	store.Handlers = append(store.Handlers, types.SliceHandlers{
		Prefix: "",
		Routes: []types.HandlerModule{
			{Route: constants.API_ROUTER_STABLE + "/graphql/ws", Method: "GET", Handler: websocket.New(hub.Handle, websocket.Config{Subprotocols: []string{"graphql-transport-ws"}})},
			{Route: constants.API_ROUTER_STABLE + "/notifications", Method: "GET", Handler: inbox.GetInbox},
		},
	})
}

// ModuleNotificationsProvider cablea el consumo de notificaciones.
//
// Va aparte de `ModuleEmailsProvider` a proposito: el flujo de correos de OTP ya
// funciona y no se toca. Los dos comparten el canal de RabbitMQ, que sigue
// proveyendose desde el modulo de correos.
func ModuleNotificationsProvider() []fx.Option {
	return []fx.Option{
		/* === INFRASTRUCTURE === */
		fx.Provide(database.NewPostgresPool),
		fx.Provide(realtime.NewNotificationHub),
		fx.Provide(func(hub *realtime.NotificationHub) ports.NotificationPublisher { return hub }),

		/* === ADAPTADORES DE SALIDA === */
		fx.Provide(outdb.NewNotificationRepository,
			func(repository *outdb.NotificationRepository) ports.NotificationRepositoryInterface {
				return repository
			}),
		fx.Provide(handlers.NewInboxHandler),

		/* === CORE === */
		fx.Provide(notifications.NewProcessNotificationUseCase),
		fx.Provide(notifications.NewNotificationDebouncer),

		/* === ADAPTADORES DE ENTRADA === */
		fx.Provide(events.NewNotificationEventConsumer),
		fx.Provide(events.NewRealtimeCommentEventConsumer),
		fx.Invoke(ConfigureNotificationWebsocket),

		fx.Invoke(func(consumer *events.NotificationEventConsumer) {
			slog.Info("Activando el consumidor de notificaciones...")
			consumer.Start()
		}),
		fx.Invoke(func(consumer *events.RealtimeCommentEventConsumer) {
			consumer.Start()
		}),
		fx.Invoke(func(debouncer *notifications.NotificationDebouncer) {
			debouncer.Start()
		}),
	}
}
