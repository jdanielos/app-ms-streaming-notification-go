package modules

import (
	"log/slog"

	"github.com/gofiber/fiber/v2" // Asegúrate de usar v2
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"github.com/streamingNotifyHub/internal/infrastructure/types"
	handlersEmail "github.com/streamingNotifyHub/internal/modules/adapters/apis/handlers/emails"
	adapters "github.com/streamingNotifyHub/internal/modules/adapters/services/emails"
	useCaseEmail "github.com/streamingNotifyHub/internal/modules/application/emails"
	"github.com/streamingNotifyHub/internal/modules/domains/ports"
	"go.uber.org/fx"
)

// ConfigureUserRoutes inyecta el Handler y el Store global
func ConfigureEmailsRoutes(otpEmail *handlersEmail.SendOtpEmaisHandler, store *types.HandlersStore, _ *config.AppSettings) {

	userModule := types.SliceHandlers{
		Prefix: "",
		Routes: []types.HandlerModule{
			{
				Route:  constants.API_ROUTER_STABLE + "/emailotp",
				Method: fiber.MethodPost,
				// En Fiber, pasamos directamente la función del controlador
				Handler: otpEmail.ExecuteSendEmailsHandlers,
			},
		},
	}

	// Agregamos al almacén global para que NewFiberServer lo registre
	store.Handlers = append(store.Handlers, userModule)
}

// ModuleUserProvider expone los proveedores para Fx
func ModuleEmailsProvider() []fx.Option {
	return []fx.Option{

		// 1. Proveemos el Handler (el controlador)
		fx.Provide(handlersEmail.NewSendOtpEmaisHandler),

		// 2. Dominios puertos
		fx.Provide(adapters.NewServicesEmailAdapter,
			func(adapter *adapters.ServicesEmailAdapter) ports.EmailsRepositoryInterface {
				return adapter
			}),

		// fx.Provide(usecases.NewUserUseCase),
		fx.Provide(useCaseEmail.NewSendOtpEmaisUseCase),

		// 3. Invocamos la configuración de rutas
		fx.Invoke(ConfigureEmailsRoutes),
	}
}

type UserHandler struct {
	// Aquí puedes inyectar usecases si quieres
}

func NewUserHandler() *UserHandler {
	return &UserHandler{}
}

// LoginHandler es el método que se registra en la ruta
func (h *UserHandler) LoginHandler(c *fiber.Ctx) error {
	// Ejemplo de respuesta JSON
	return c.JSON(fiber.Map{
		"status": "success",
		"msg":    "Login de prueba",
	})
}

func (h *UserHandler) GetProfileHandler(c *fiber.Ctx) error {
	slog.Debug("Ok")
	//user := fiber.Map{"user": "Admin"}
	return config.NewAuthInvalidCredentials("Perfil obtenido correctamente")
	//return config.SendOk(c, user, "Perfil obtenido correctamente")
}
