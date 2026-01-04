package ports

import (
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/email"
)

type EmailsRepositoryInterface interface {
	SendOtpsEmails(data command.SendOtpCommandRequest, html string) (email.EntityEmailOtpResponse, error)
}
