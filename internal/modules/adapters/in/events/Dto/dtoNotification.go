package dto

import (
	"encoding/json"
	"errors"
	"time"
)

// NotificationEventDTO es el contrato que publican los demas microservicios.
//
// Se mantiene aparte de `AuthEventDTO`, que sigue sirviendo al flujo de correos
// de OTP mientras los emisores migran. Cuando no queden emisores del viejo, ese
// se borra.
type NotificationEventDTO struct {
	// Identificador del evento en el servicio origen. TIENE que ser estable
	// entre reintentos del emisor: es lo unico que impide guardar la misma
	// notificacion dos veces.
	EventID       string `json:"event_id"`
	SourceService string `json:"source_service"`
	EventType     string `json:"event_type"`
	CategoryCode  string `json:"category_code"`

	UserID      string `json:"user_id"`
	ActorUserID string `json:"actor_user_id"`

	Title     string `json:"title"`
	Body      string `json:"body"`
	ActionURL string `json:"action_url"`

	ExpiresAt *time.Time `json:"expires_at"`

	// Carga libre del emisor. Se guarda tal cual para poder reprocesar sin
	// volver a pedirle nada al servicio que la mando.
	Payload json.RawMessage `json:"payload"`
}

var (
	ErrMissingEventID  = errors.New("event_id vacio")
	ErrMissingSource   = errors.New("source_service vacio")
	ErrMissingCategory = errors.New("category_code vacio")
	ErrMissingUser     = errors.New("user_id vacio")
	ErrMissingTitle    = errors.New("title vacio")
)

// Validate comprueba lo que hace falta para poder guardar la notificacion.
//
// Un mensaje que no pasa esta validacion no mejora por reintentarlo, asi que el
// consumidor lo manda directo a la DLQ en vez de reencolarlo.
func (d NotificationEventDTO) Validate() error {
	switch {
	case d.EventID == "":
		return ErrMissingEventID
	case d.SourceService == "":
		return ErrMissingSource
	case d.CategoryCode == "":
		return ErrMissingCategory
	case d.UserID == "":
		return ErrMissingUser
	case d.Title == "":
		return ErrMissingTitle
	}
	return nil
}

// RawPayload devuelve la carga original, o un objeto vacio si el emisor no mando
// ninguna. La columna es `NOT NULL`, asi que no puede quedarse en nil.
func (d NotificationEventDTO) RawPayload() []byte {
	if len(d.Payload) == 0 {
		return []byte("{}")
	}
	return d.Payload
}
