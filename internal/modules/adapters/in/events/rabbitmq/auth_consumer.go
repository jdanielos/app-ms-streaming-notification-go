package events

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	dto "github.com/streamingNotifyHub/internal/modules/adapters/in/events/Dto"
	"github.com/streamingNotifyHub/internal/modules/core/notifications"
)

// configura el evento para consumir las pubblicaciones
// llama al core del servicio
type AuthEventConsumer struct {
	channel        *amqp091.Channel
	queueName      *string
	notifyServices *notifications.NotificationServices
	isPaused       bool
	mu             sync.Mutex // asegurar concurrencia acceso unica vez por peticion
}

// crea el "constructor" para pasar la informacion donde se llame ene este caso en fx(server)
func NewAuthEventConsumer(channel *amqp091.Channel, queueName *string, notifyServices *notifications.NotificationServices) *AuthEventConsumer {
	return &AuthEventConsumer{
		channel:        channel,
		queueName:      queueName,
		notifyServices: notifyServices,
	}
}

// metodo asociado a AuthEventConsumer
// escuchamos los eventos y se encolan el mensage para se procesado 1 por 1
func (c *AuthEventConsumer) StartNofifyServices() {

	msgs, err := c.channel.Consume(*c.queueName, "", false, false, false, false, nil)
	if err != nil {
		slog.Error("Error al registrar el canal")
	}

	for i := 0; i < 5; i++ {
		go func(workerID int) {
			for d := range msgs {

				// bloqueo de mensageria
				// validamos si se encuentra pausado por motivos si el servicio fallo
				// si esta pausado lo desbloqueamos para que pueda llegar otro mensage sin interrupciones
				// y validar nuevamente el mensage
				// Nack con requeue=true lo pone otra vez al principio de la cola
				c.mu.Lock()
				if c.isPaused {
					c.mu.Unlock()
					d.Nack(false, true)
					time.Sleep(5 * time.Second)
					continue
				}

				c.mu.Unlock()
				var dto dto.AuthEventDTO

				if err := json.Unmarshal(d.Body, &dto); err != nil {
					d.Ack(false)
					continue
				}
				cmd := dto.ToCommand()

				_, err := c.notifyServices.SendOtpService(&cmd)

				if err != nil {
					slog.Warn("API falló, enviando a reintento", "worker", workerID, "error", err)
					c.mu.Lock()
					c.isPaused = true
					c.mu.Unlock()
					// Nack con requeue=false
					// Si configuraste DLX en la cola, esto lo mandará a la cola de espera de 10 min
					d.Nack(false, true)

					// programacion de 10 minutos si la api falla
					go func() {

						time.Sleep(10 * time.Minute)
						c.mu.Lock()
						c.isPaused = false
						c.mu.Unlock()
						slog.Info("Pausa terminada. Reintentando procesamiento...")
					}()
				} else {
					slog.Info("Procesado con éxito", "worker", workerID)
					d.Ack(false) // se confirma que todo salió bien
				}
			}
		}(i)
	}
}
