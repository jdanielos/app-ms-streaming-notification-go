package adapters

import (
	"errors"
	"os"

	"github.com/resend/resend-go/v2"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/email"
)

type ServicesEmailAdapter struct {
	// data
}

func NewServicesEmailAdapter() *ServicesEmailAdapter {
	return &ServicesEmailAdapter{}
}

// Esta función hace que EmailPostgresAdapter CUMPLA con la interfaz ports.EmailsRepositoryInterface
func (a *ServicesEmailAdapter) SendOtpsEmails(data command.SendOtpCommandRequest, html string) (email.EntityEmailOtpResponse, error) {

	client := resend.NewClient(os.Getenv("CREDENTIALS_EMAIL_PROVIDER"))

	params := &resend.SendEmailRequest{
		From:    os.Getenv("CREDENTIALS_FROM_EMAIL_PROVIDER"),
		To:      []string{data.Email},
		Subject: data.Subject,
		Html:    html,
	}

	sent, err := client.Emails.Send(params)

	if err != nil {
		println(err.Error())
		return email.EntityEmailOtpResponse{}, errors.New(err.Error())
	}

	return email.EntityEmailOtpResponse{
		Message: "Correo enviado exitosamente a " + data.Email,
		Details: struct {
			ServiceExternal          string "json:\"service_external\""
			ResponseServicesExternal string "json:\"responses_ervices_external\""
		}{
			ServiceExternal:          params.ScheduledAt,
			ResponseServicesExternal: sent.Id,
		},
	}, nil
}
