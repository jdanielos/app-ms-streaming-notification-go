package modules

import (
	"log/slog"

	"github.com/gofiber/fiber/v2" // Asegúrate de usar v2
	"github.com/streamingNotifyHub/internal/infrastructure/brokers"
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"github.com/streamingNotifyHub/internal/infrastructure/types"
	events "github.com/streamingNotifyHub/internal/modules/adapters/in/events/rabbitmq"
	handlersEmail "github.com/streamingNotifyHub/internal/modules/adapters/in/handlers/emails"
	adapters "github.com/streamingNotifyHub/internal/modules/adapters/out/services/emails"
	coreEmail "github.com/streamingNotifyHub/internal/modules/core/emails"
	"github.com/streamingNotifyHub/internal/modules/core/notifications"
	"github.com/streamingNotifyHub/internal/modules/domains/ports"
	"go.uber.org/fx"
)

// ConfigureUserRoutes inyecta el Handler y el Store global
func ConfigureEmailsRoutes(
	otpEmail *handlersEmail.SendOtpEmaisHandler,
	otpEmailClient *handlersEmail.SendClientOtpEmaisHandler,
	store *types.HandlersStore, _ *config.AppSettings) {

	userModule := types.SliceHandlers{
		Prefix: "",
		Routes: []types.HandlerModule{
			{
				Route:   constants.API_ROUTER_STABLE + "/emailotp",
				Method:  fiber.MethodPost,
				Handler: otpEmail.ExecuteSendEmailsHandlers,
			},
			{
				Route:   constants.API_ROUTER_STABLE + "/emailotpRegister",
				Method:  fiber.MethodPost,
				Handler: otpEmailClient.ExecuteSendClientEmailsHandlers,
			},
		},
	}

	// Agregamos al almacén global para que NewFiberServer lo registre
	store.Handlers = append(store.Handlers, userModule)
}

// ModuleUserProvider expone los proveedores para Fx
func ModuleEmailsProvider() []fx.Option {
	return []fx.Option{

		/* === INFRASTRUCTURE ===*/
		fx.Provide(brokers.NewRabbitMQChannel),

		// 1. Proveemos el Handler (el controlador)
		fx.Provide(handlersEmail.NewSendOtpEmaisHandler),
		fx.Provide(handlersEmail.NewSendClientOtpEmaisHandler),
		fx.Provide(events.NewAuthEventConsumer),

		// 2. Dominios puertos
		fx.Provide(adapters.NewServicesEmailAdapter,
			func(adapter *adapters.ServicesEmailAdapter) ports.EmailsRepositoryInterface {
				return adapter
			}),
		fx.Provide(adapters.NewEmailServicesAdapter,
			func(adapter *adapters.EmailServicesAdapter) ports.EventEmailsInterface {
				return adapter
			}),

		// fx.Provide(usecases.NewUserUseCase),
		fx.Provide(coreEmail.NewSendOtpEmaisUseCase),
		fx.Provide(coreEmail.NewSendClientOtpEmaisUseCase),
		fx.Provide(notifications.NewSendClientOtpEmaisUseCase),

		// 3. Invocamos la configuración de rutas
		fx.Invoke(ConfigureEmailsRoutes),
		fx.Invoke(func(consumer *events.AuthEventConsumer) {
			slog.Info("Activando el consumidor de RabbitMQ...")
			consumer.StartNofifyServices()
		}),
	}
}
