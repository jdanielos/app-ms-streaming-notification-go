package ports

import (
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/email"
)

type EventEmailsInterface interface {
	SendOtpsEmail(data *command.AuthenticatedUserCommand, html string) (email.EntityEmailOtpResponse, error)
}
