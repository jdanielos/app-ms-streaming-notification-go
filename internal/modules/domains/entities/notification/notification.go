package notification

import "time"

// Canales de entrega. Coinciden con el CHECK de `notification_deliveries.channel`.
const (
	ChannelInApp = "in_app"
	ChannelEmail = "email"
	ChannelPush  = "push"
	ChannelMQTT  = "mqtt"
)

// Estados de la notificacion. Coinciden con el CHECK de `notifications.status`.
const (
	StatusPending   = "pending"
	StatusProcessed = "processed"
	StatusFailed    = "failed"
	StatusDiscarded = "discarded"
)

// Estados de una entrega concreta.
const (
	DeliveryPending = "pending"
	DeliverySent    = "sent"
	DeliveryFailed  = "failed"
	// El usuario tiene el canal apagado y la categoria no es obligatoria. Se
	// registra igual: "no se envio, y a proposito" es informacion, y sin ella
	// una entrega ausente no se distingue de una perdida.
	DeliverySkipped = "skipped"
)

// Category describe que es una notificacion y por donde puede salir.
type Category struct {
	Code        string
	AllowsInApp bool
	AllowsEmail bool
	AllowsPush  bool
	// Ignora las preferencias del usuario. Solo seguridad de la cuenta.
	IsMandatory bool
	IsActive    bool
}

// UserSettings son las preferencias que ya guarda `user_notification_settings`.
type UserSettings struct {
	UserID             string
	EmailNotifications bool
	PushNotifications  bool
}

// Notification es un evento del bus ya resuelto a un usuario.
type Notification struct {
	NotificationID string
	UserID         string
	CategoryCode   string

	SourceService string
	EventType     string
	// Identificador del evento en el servicio origen. Es la clave de
	// idempotencia: RabbitMQ entrega al menos una vez.
	EventID string

	// Direccion completa acordada: mqtt://{topic}/message/{user_id}/{categoria}
	Topic      string
	RoutingKey string

	Title       string
	Body        string
	ActionURL   string
	ActorUserID string
	// Mensaje original sin transformar, en JSON.
	Payload []byte

	Status    string
	ExpiresAt *time.Time
	CreatedAt time.Time
}

// Delivery es un intento de entrega por un canal concreto.
type Delivery struct {
	NotificationID string
	Channel        string
	// Direccion usada: topic, correo o token de dispositivo. Se congela en el
	// momento del envio.
	Terminal string
	Status   string
}

type InboxItem struct {
	NotificationID string  `json:"notificationId"`
	UserID         string  `json:"userId"`
	CategoryCode   string  `json:"categoryCode"`
	Title          string  `json:"title"`
	Body           string  `json:"body,omitempty"`
	ActionURL      string  `json:"actionUrl,omitempty"`
	ActorUserID    string  `json:"actorUserId,omitempty"`
	ActorAvatarURL *string `json:"actorAvatarUrl"`
	ReadAt         *string `json:"readAt"`
	CreatedAt      string  `json:"createdAt"`
}

// CategorySummary es una categoria del catalogo con lo que ese usuario tiene
// dentro. Va junta —nombre y recuento— porque la bandeja las pinta como filtros:
// un filtro sin su cifra obliga a pulsarlo para saber si tiene algo.
type CategorySummary struct {
	Code          string `json:"code"`
	DisplayNameEs string `json:"displayNameEs"`
	DisplayNameEn string `json:"displayNameEn"`
	Total         int    `json:"total"`
	Unread        int    `json:"unread"`
}

type InboxPage struct {
	Items      []InboxItem `json:"items"`
	NextCursor *string     `json:"nextCursor"`
	HasMore    bool        `json:"hasMore"`
}

// BuildTopic arma la direccion del canal en el formato acordado. Vive aqui y no
// repartido por los adaptadores para que exista un unico sitio donde cambiarlo.
func BuildTopic(base, userID, categoryCode string) string {
	return "mqtt://" + base + "/message/" + userID + "/" + categoryCode
}

// ResolveChannels decide por donde sale la notificacion.
//
// El cruce es: lo que la categoria admite Y lo que el usuario acepta. Con una
// excepcion — si la categoria es obligatoria, las preferencias no se consultan.
// Quien apaga los avisos no puede quedarse sin enterarse de que le entraron a la
// cuenta.
//
// Devuelve todos los canales que la categoria admite, marcando como `skipped`
// los que el usuario rechaza: asi queda constancia de la decision.
func ResolveChannels(category Category, settings UserSettings) map[string]string {
	channels := map[string]string{}

	decide := func(allowed, accepted bool) (string, bool) {
		if !allowed {
			return "", false
		}
		if category.IsMandatory || accepted {
			return DeliveryPending, true
		}
		return DeliverySkipped, true
	}

	// La copia en la campana no depende de ninguna preferencia: es la bandeja
	// del propio producto, no un envio a un tercero.
	if category.AllowsInApp {
		channels[ChannelInApp] = DeliveryPending
	}
	if status, ok := decide(category.AllowsEmail, settings.EmailNotifications); ok {
		channels[ChannelEmail] = status
	}
	if status, ok := decide(category.AllowsPush, settings.PushNotifications); ok {
		channels[ChannelPush] = status
	}

	return channels
}
