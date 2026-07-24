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
	// 1. Conexión
	conn, err := amqp091.Dial(os.Getenv(constants.ENV_RABBITMQ_URL))
	if err != nil {
		slog.Error("No se pudo conectar a RabbitMQ", "error", err)
		return nil, nil
	}

	// 2. Canal
	ch, err := conn.Channel()
	if err != nil {
		slog.Error("No se pudo abrir el canal", "error", err)
		conn.Close()
		return nil, nil
	}

	// microservicios publican mensages en esta direccion
	exchangeName := os.Getenv(constants.ENV_CHANELRABBITMQ_NOTIFY_TOPIC)
	if exchangeName == "" {
		slog.Error("Variable de exchange RabbitMQ no configurada", "name", constants.ENV_CHANELRABBITMQ_NOTIFY_TOPIC)
		ch.Close()
		conn.Close()
		return nil, nil
	}
	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		slog.Error("Error declarando exchange", "error", err)
		return nil, nil
	}

	queueName := os.Getenv(constants.ENV_CHANELRABBITMQ_NOTIFY_RMSG)

	// donde se lee los mensages de RabbitMQ
	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		slog.Error("Error declarando cola", "name", queueName, "error", err)
		return nil, nil
	}

	err = ch.QueueBind(
		queueName,
		"#",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		slog.Error("Error vinculando cola al exchange", "queue", queueName, "exchange", exchangeName, "error", err)
		ch.Close()
		conn.Close()
		return nil, nil
	}

	ch.Qos(1, 0, false)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slog.Info("Cerrando conexión de RabbitMQ...")
			ch.Close()
			return conn.Close()
		},
	})

	return ch, &queueName
}
