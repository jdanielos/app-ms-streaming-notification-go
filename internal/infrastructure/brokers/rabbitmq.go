package brokers

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/rabbitmq/amqp091-go"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"go.uber.org/fx"
)

// NewRabbitMQChannel devuelve error en vez de nil silencioso.
//
// Antes cada fallo hacia `return nil, nil`. fx no tiene forma de saber que eso
// es un fallo, asi que inyectaba el canal nil y el proceso reventaba mucho
// despues, dentro del consumidor, con un panic que no dice nada de la causa.
// Devolviendo error, fx aborta el arranque y escribe el motivo real.
func NewRabbitMQChannel(lc fx.Lifecycle) (*amqp091.Channel, *string, error) {
	// 1. Conexión
	conn, err := amqp091.Dial(os.Getenv(constants.ENV_RABBITMQ_URL))
	if err != nil {
		return nil, nil, fmt.Errorf("no se pudo conectar a RabbitMQ: %w", err)
	}

	// 2. Canal
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("no se pudo abrir el canal: %w", err)
	}

	// microservicios publican mensages en esta direccion
	exchangeName := os.Getenv(constants.ENV_CHANELRABBITMQ_NOTIFY_TOPIC)
	if exchangeName == "" {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("variable %s no configurada", constants.ENV_CHANELRABBITMQ_NOTIFY_TOPIC)
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
		return nil, nil, fmt.Errorf("declarando exchange %q: %w", exchangeName, err)
	}

	queueName := os.Getenv(constants.ENV_CHANELRABBITMQ_NOTIFY_RMSG)

	// Dead letter exchange y su cola. Es el destino de lo que no se pudo
	// procesar: JSON roto, categoria inexistente o reintentos agotados.
	//
	// Sin esto la unica salida ante un fallo era `Nack(requeue=true)`, que
	// devuelve el mensaje al principio de la cola; los workers lo recogen al
	// instante y forman un bucle caliente del que nada mas avanza. Con la DLQ el
	// mensaje se aparta y la cola sigue.
	err = ch.ExchangeDeclare(constants.CHANELRABBITMQ_NOTIFY_DLX, "fanout", true, false, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("declarando dead letter exchange: %w", err)
	}

	_, err = ch.QueueDeclare(constants.CHANELRABBITMQ_NOTIFY_DLQ, true, false, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("declarando dead letter queue: %w", err)
	}

	if err = ch.QueueBind(constants.CHANELRABBITMQ_NOTIFY_DLQ, "", constants.CHANELRABBITMQ_NOTIFY_DLX, false, nil); err != nil {
		return nil, nil, fmt.Errorf("vinculando dead letter queue: %w", err)
	}

	// donde se lee los mensages de RabbitMQ
	//
	// Se intenta declarar con la dead letter exchange. Si la cola YA existe sin
	// ese argumento, RabbitMQ responde PRECONDITION_FAILED (406) y no deja
	// cambiarlo: los argumentos de una cola son inmutables.
	//
	// Ante eso NO se aborta el arranque. Un servicio que ya estaba funcionando no
	// puede dejar de arrancar por una mejora de robustez; se sigue con la cola
	// como esta y se avisa. La DLQ empieza a funcionar cuando la cola se recree.
	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp091.Table{
			"x-dead-letter-exchange": constants.CHANELRABBITMQ_NOTIFY_DLX,
		},
	)
	if err != nil {
		// Un error 406 cierra el canal por parte del servidor. Cualquier
		// operacion posterior sobre el fallaria, asi que hace falta uno nuevo.
		ch, err = conn.Channel()
		if err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("reabriendo el canal tras el fallo de declaracion: %w", err)
		}

		// Segundo intento con la configuracion que la cola ya tiene.
		if _, err = ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return nil, nil, fmt.Errorf("declarando la cola %q: %w", queueName, err)
		}

		slog.Warn("la cola existe sin dead letter exchange: los mensajes descartados NO van a la DLQ",
			slog.String("cola", queueName),
			slog.String("solucion", "borrar la cola una vez, vacia, para que se recree con la DLX"),
		)
	}

	err = ch.QueueBind(
		queueName,
		"#",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("vinculando cola %q al exchange %q: %w", queueName, exchangeName, err)
	}

	// El prefetch lo fija cada consumidor al arrancar, segun cuantos workers
	// levante. Fijarlo aqui en 1 obligaba a todos a ir de uno en uno.

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slog.Info("Cerrando conexión de RabbitMQ...")
			ch.Close()
			return conn.Close()
		},
	})

	return ch, &queueName, nil
}
