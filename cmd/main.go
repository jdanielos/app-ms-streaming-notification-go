package main

import (
	"github.com/joho/godotenv"
	"github.com/streamingNotifyHub/internal/infrastructure/server"
	"github.com/streamingNotifyHub/internal/modules"
)

func main() {
	godotenv.Overload()
	app := server.ProviderServerStore{}
	app.Init()
	app.AddModule(modules.ModuleEmailsProvider())
	app.Up()

}
