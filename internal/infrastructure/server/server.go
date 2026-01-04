package server

import (
	"context"
	"fmt"
	"io" // Necesario para escribir en dos sitios
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/infrastructure/types"
	"go.uber.org/fx"
)

func setRoutersByModules(app *fiber.App, h *types.HandlersStore, _ *config.AppSettings) {
	for _, handlerModule := range h.Handlers {
		group := app.Group("/" + handlerModule.Prefix)
		for _, routersItems := range handlerModule.Routes {
			group.Add(routersItems.Method, routersItems.Route, routersItems.Handler)
		}
	}
}

func NewFiberServer(lc fx.Lifecycle, h *types.HandlersStore, appsettings *config.AppSettings) *fiber.App {
	// 1. Preparar logs
	logDir := "logs"
	os.MkdirAll(logDir, 0755)
	logFileName := fmt.Sprintf("%s/%s.log", logDir, time.Now().Format("2006-01-02"))
	logFile, _ := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	// 2. MULTIWRITER:  CONSOLA y ARCHIVO
	multiWriter := io.MultiWriter(os.Stdout, logFile)

	// 3. Configurar slog (JSON Estándar)
	mySlog := slog.New(slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
	})).With(
		slog.String("app", "streamingNotifyHub"),
		slog.Int("pid", os.Getpid()),
	)
	slog.SetDefault(mySlog)

	app := fiber.New(fiber.Config{
		AppName: "streamingNotifyHub",
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			resp := config.ResponseSystem{
				Success:      false,
				Message:      err.Error(),
				InternalCode: "ERROR_SYSTEM",
				Code:         5000,
				Status:       500,
			}

			if e, ok := err.(*config.AppError); ok {
				resp.Code = e.Code
				resp.InternalCode = e.InternalCode
				resp.Status = e.Status
				code = e.Status
			}

			return c.Status(code).JSON(resp)
		},
	})
	// 4. MIDDLEWARES (En el orden correcto)
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,PATCH,OPTIONS,MERGE",
	}))
	app.Use(recover.New())

	// 5. LOGGER DE PETICIONES (Debe ir ANTES de las rutas)
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next() // Procesar la ruta
		slog.Debug("ssss")
		slog.Info("http_request",
			slog.String("ip", c.IP()),
			slog.String("method", c.Method()),
			slog.String("path", c.Path()),
			slog.Int("status", c.Response().StatusCode()),
			slog.Float64("latency_ms", float64(time.Since(start).Microseconds())/1000.0),
		)
		return err
	})

	setRoutersByModules(app, h, appsettings)

	// El 404 siempre va al FINAL
	app.Use(func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "not found"})
	})

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				certPath, _ := filepath.Abs("localhost.pem")
				keyPath, _ := filepath.Abs("localhost-key.pem")

				slog.Info("Cargando certificados desde", "cert", certPath, "key", keyPath)

				// Fíjate en el ".pem" al final de ambos archivos
				if err := app.ListenTLS(":3002", "localhost.pem", "localhost-key.pem"); err != nil {
					slog.Error("Error Fiber ListenTLS", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			slog.Info("Apagando servidor...")
			logFile.Close()
			return app.Shutdown()
		},
	})

	slog.Info("Listening on server HTTPS/2 -  http://" + appsettings.Config.Host + ":" + appsettings.Config.Port)
	return app
}

func NewHandlersStoreProvider() *types.HandlersStore {
	return types.NewHandlersStore()
}

func ProvideHandlersStore() fx.Option {
	return fx.Provide(NewHandlersStoreProvider)
}
