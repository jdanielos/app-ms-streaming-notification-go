package main

import (
	"github.com/streamingNotifyHub/internal/infrastructure/server"
	"github.com/streamingNotifyHub/internal/modules"
)

func main() {
	app := server.ProviderServerStore{}
	app.Init()
	app.AddModule(modules.ModuleEmailsProvider())
	app.Up()

}
