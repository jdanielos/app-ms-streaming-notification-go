package server

import (
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/infrastructure/types"
	"go.uber.org/fx"
)

type ProviderServerStore struct {
	Providers []fx.Option
}

// Init the providers
func (ps *ProviderServerStore) Init() {
	// Add the providers to the list
	ps.Providers = []fx.Option{

		fx.Provide(types.NewHandlersStore),
		fx.Provide(config.AppSettingsUnmarshalnFn),
	}
}
func (ps *ProviderServerStore) AddModule(p []fx.Option) {

	ps.Providers = append(ps.Providers, p...)
}

// Up the server
func (ps *ProviderServerStore) Up(lp ...[]fx.Option) {
	ps.Providers = append(ps.Providers, fx.Invoke(NewFiberServer))
	fx.New(ps.Providers...).Run()
}
