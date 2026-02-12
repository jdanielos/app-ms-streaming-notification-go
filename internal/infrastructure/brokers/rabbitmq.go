package brokers

import (
	"context"
	"log/slog"
	"os"

	"github.com/rabbitmq/amqp091-go"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"go.uber.org/fx"
)

func NewRabbitMQChannel(lc fx.Lifecycle) (*amqp091.Channel, *string) {
	conn, err := amqp091.Dial(os.Getenv(constants.ENV_RABBITMQ_URL))
	if err != nil {
		slog.Error("No se pudo conectar a RabbitMQ", "error", err)
		return nil, nil
	}

	ch, err := conn.Channel()
	if err != nil {
		slog.Error("No se pudo abrir el canal", "error", err)
		return nil, nil
	}
	err = ch.Qos(
		1,     // mensages soportados 4 al mismo tiempo
		0,     // limite de bytes en este caso soporta grandes cantidades de mensages
		false, // global para todos los caneles compartidos
	)
	if err != nil {
		slog.Error("No se pudo abrir el canal", "error", err)
		return nil, nil
	}

	queueName := os.Getenv(constants.ENV_CHANELRABBITMQ_URL)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slog.Info("Cerrando conexión de RabbitMQ...")
			ch.Close()
			return conn.Close()
		},
	})

	println(queueName, "kkk")

	return ch, &queueName
}
