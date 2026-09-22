package brokers

import (
	"fmt"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

// ChannelFactory abre un canal NUEVO contra RabbitMQ, redialando la conexion
// si hace falta.
//
// Por que existe: `amqp091.Channel` no se recupera solo. Cuando RabbitMQ
// cierra la conexion (se reinicia, se cae, o el servidor cierra el canal por
// un error de protocolo), el canal queda muerto para siempre - volver a
// llamar `Consume` sobre el mismo objeto nunca funciona, sale
// "channel/connection is not open" cada vez. Los consumidores que necesiten
// recuperarse de verdad piden un canal nuevo con esto en vez de reintentar
// sobre el que ya tienen.
type ChannelFactory func() (*amqp091.Channel, error)

// rabbitConnectionManager guarda la conexion activa y la redialea cuando se
// detecta cerrada. Un solo manejador, compartido por todos los consumidores -
// no tiene sentido que cada uno mantenga su propia conexion a RabbitMQ.
type rabbitConnectionManager struct {
	mu   sync.Mutex
	conn *amqp091.Connection
	url  string
}

func newRabbitConnectionManager(url string, initialConn *amqp091.Connection) *rabbitConnectionManager {
	return &rabbitConnectionManager{conn: initialConn, url: url}
}

// Channel devuelve un canal nuevo. Si la conexion actual esta cerrada
// (`IsClosed`), redialea antes de intentar abrir el canal - as suele pasar
// cuando el broker se reinicio y no solo el canal, sino la conexion entera,
// murio.
func (m *rabbitConnectionManager) Channel() (*amqp091.Channel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn == nil || m.conn.IsClosed() {
		conn, err := amqp091.Dial(m.url)
		if err != nil {
			return nil, fmt.Errorf("redialing rabbitmq: %w", err)
		}
		m.conn = conn
	}

	ch, err := m.conn.Channel()
	if err != nil {
		// La conexion decia estar viva pero abrir el canal fallo igual - se
		// descarta para que el PROXIMO intento redialee en vez de repetir el
		// mismo error para siempre contra una conexion que en realidad ya
		// esta rota.
		m.conn = nil
		return nil, fmt.Errorf("opening rabbitmq channel: %w", err)
	}

	return ch, nil
}

func (m *rabbitConnectionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn == nil {
		return nil
	}
	return m.conn.Close()
}
