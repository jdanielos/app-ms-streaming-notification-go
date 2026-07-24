package main

import (
	"log/slog"
	"os"

	"github.com/joho/godotenv"
	"github.com/streamingNotifyHub/internal/infrastructure/server"
	"github.com/streamingNotifyHub/internal/modules"
)

func main() {
	godotenv.Overload()
	apiKey := os.Getenv("CREDENTIALS_EMAIL_PROVIDER")
	fromEmail := os.Getenv("CREDENTIALS_FROM_EMAIL_PROVIDER")
	workingDirectory, _ := os.Getwd()
	apiKeyPrefix := ""
	if len(apiKey) >= 7 {
		apiKeyPrefix = apiKey[:7]
	}
	slog.Info("brevo configuration loaded",
		slog.Bool("api_key_configured", apiKey != ""),
		slog.String("api_key_prefix", apiKeyPrefix),
		slog.String("from_email", fromEmail),
		slog.String("working_directory", workingDirectory),
	)

	app := server.ProviderServerStore{}
	app.Init()
	app.AddModule(modules.ModuleEmailsProvider())
	app.Up()

}
