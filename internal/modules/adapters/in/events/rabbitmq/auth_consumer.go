package events

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/streamingNotifyHub/internal/infrastructure/brokers"
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
	// Pide un canal NUEVO cuando el actual muere - ver la nota de
	// `ChannelFactory` en el paquete brokers. Sin esto, si Rabbit se caia
	// este consumidor se quedaba en silencio para siempre y los correos de
	// OTP dejaban de salir sin ningun aviso.
	newChannel brokers.ChannelFactory
}

// crea el "constructor" para pasar la informacion donde se llame ene este caso en fx(server)
func NewAuthEventConsumer(channel *amqp091.Channel, newChannel brokers.ChannelFactory, queueName *string, notifyServices *notifications.NotificationServices) *AuthEventConsumer {
	return &AuthEventConsumer{
		channel:        channel,
		newChannel:     newChannel,
		queueName:      queueName,
		notifyServices: notifyServices,
	}
}

// metodo asociado a AuthEventConsumer
// escuchamos los eventos y se encolan el mensage para se procesado 1 por 1
func (c *AuthEventConsumer) StartNofifyServices() {

	// Sin esta guarda, un canal nil revienta con un panic que apunta aqui y no
	// dice nada del motivo real, que esta en la construccion del broker.
	if c.channel == nil || c.queueName == nil {
		slog.Error("consumidor de auth sin canal de RabbitMQ, no se arranca")
		return
	}

	msgs, err := c.channel.Consume(*c.queueName, "", false, false, false, false, nil)
	if err != nil {
		slog.Error("Error al registrar el canal")
		return
	}

	go func() {
		// Mismo motivo que los otros dos consumidores: el `range` de cada
		// worker TERMINA cuando RabbitMQ cierra la conexion, y reintentar
		// sobre el mismo canal ya muerto nunca se recupera - hace falta uno
		// nuevo, pedido con `newChannel()`.
		for {
			c.runWorkers(msgs)

			slog.Error("auth_consumer_stream_closed", "queue", *c.queueName)

			for espera := time.Second; ; {
				newCh, err := c.newChannel()
				if err == nil {
					msgs, err = newCh.Consume(*c.queueName, "", false, false, false, false, nil)
				}
				if err == nil {
					c.channel = newCh
					slog.Info("auth_consumer_resumed", "queue", *c.queueName)
					break
				}
				slog.Error("auth_consumer_resume_failed", "error", err, "retry_in", espera.String())
				time.Sleep(espera)
				if espera < 30*time.Second {
					espera *= 2
				}
			}
		}
	}()
}

// runWorkers levanta los 5 workers de siempre y bloquea hasta que todos
// terminan - exactamente cuando `msgs` se cierra por una conexion caida.
func (c *AuthEventConsumer) runWorkers(msgs <-chan amqp091.Delivery) {
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
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
	wg.Wait()
}
