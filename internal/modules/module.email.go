package modules

import (
	"log/slog"

	"github.com/gofiber/fiber/v2" // Asegúrate de usar v2
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/infrastructure/types"
	"go.uber.org/fx"
)

// ConfigureUserRoutes inyecta el Handler y el Store global
func ConfigureEmailsRoutes(h *UserHandler, store *types.HandlersStore, _ *config.AppSettings) {

	userModule := types.SliceHandlers{
		Prefix: "users",
		Routes: []types.HandlerModule{
			{
				Route:  "/login",
				Method: fiber.MethodPost,
				// En Fiber, pasamos directamente la función del controlador
				Handler: h.LoginHandler,
			},
			{
				Route:   "/profile",
				Method:  fiber.MethodGet,
				Handler: h.GetProfileHandler,
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
		fx.Provide(NewUserHandler),

		// 2. Podrías proveer Usecases o DAOs aquí igual que en el otro proyecto
		// fx.Provide(usecases.NewUserUseCase),

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
