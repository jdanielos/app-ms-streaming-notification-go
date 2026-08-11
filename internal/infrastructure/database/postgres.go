package database

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"go.uber.org/fx"
)

const (
	// Diez conexiones. El hub no atiende trafico de usuario: consume de una cola
	// con un numero acotado de workers, asi que el pool solo tiene que cubrir a
	// esos workers mas el de entrega y las lecturas de la bandeja.
	maxConnections = 10

	// Dos en reserva para que el primer mensaje despues de un rato quieto no
	// pague el coste de abrir la conexion.
	minConnections = 2

	// Una conexion viva indefinidamente acaba cortada por el servidor o por un
	// cortafuegos intermedio, y el fallo aparece como un error suelto y raro.
	// Reciclarla antes evita perseguir ese fantasma.
	maxConnLifetime = time.Hour
	maxConnIdleTime = 30 * time.Minute

	// Si la base no responde al arrancar, es mejor fallar rapido y que el
	// orquestador reinicie que quedarse colgado sin decir nada.
	connectTimeout = 10 * time.Second
)

// NewPostgresPool crea el pool contra la base de usuarios y lo ata al ciclo de
// vida de fx: se comprueba al arrancar y se cierra ordenadamente al parar.
func NewPostgresPool(lc fx.Lifecycle) (*pgxpool.Pool, error) {
	url := os.Getenv(constants.ENV_DATABASE_USERS_URL)
	if url == "" {
		return nil, fmt.Errorf("variable %s no configurada", constants.ENV_DATABASE_USERS_URL)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("cadena de conexion invalida: %w", err)
	}

	cfg.MaxConns = maxConnections
	cfg.MinConns = minConnections
	cfg.MaxConnLifetime = maxConnLifetime
	cfg.MaxConnIdleTime = maxConnIdleTime

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("no se pudo crear el pool: %w", err)
	}

	// `NewWithConfig` no abre nada todavia. Sin este ping, una contraseña mal
	// puesta no se descubre hasta el primer mensaje, y ahi ya parece un fallo
	// del consumidor.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no responde la base de usuarios: %w", err)
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			slog.Info("cerrando el pool de postgres")
			pool.Close()
			return nil
		},
	})

	slog.Info("pool de postgres listo",
		slog.Int("max_conns", int(cfg.MaxConns)),
		slog.Int("min_conns", int(cfg.MinConns)),
	)

	return pool, nil
}
