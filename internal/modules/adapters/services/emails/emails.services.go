package adapters

import (
	"context"
	"errors"
	"os"

	brevo "github.com/getbrevo/brevo-go/lib"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/email"
)

type ServicesEmailAdapter struct {
	// Puedes inicializar el cliente aquí si prefieres no hacerlo en cada llamada
}

func NewServicesEmailAdapter() *ServicesEmailAdapter {
	return &ServicesEmailAdapter{}
}

func (a *ServicesEmailAdapter) SendOtpsEmails(data command.SendOtpCommandRequest, html string) (email.EntityEmailOtpResponse, error) {

	cfg := brevo.NewConfiguration()
	cfg.AddDefaultHeader("api-key", os.Getenv("CREDENTIALS_EMAIL_PROVIDER"))
	client := brevo.NewAPIClient(cfg)

	params := brevo.SendSmtpEmail{
		Sender: &brevo.SendSmtpEmailSender{
			Name:  "Seguridad",
			Email: os.Getenv("CREDENTIALS_FROM_EMAIL_PROVIDER"),
		},
		To: []brevo.SendSmtpEmailTo{
			{
				Email: data.Email,
			},
		},
		Subject:     data.Subject,
		HtmlContent: html,
	}

	result, _, err := client.TransactionalEmailsApi.SendTransacEmail(context.Background(), params)

	if err != nil {
		println("Error Brevo:", err.Error())
		return email.EntityEmailOtpResponse{}, errors.New(`en estos momentos nuestro equipo esta trabajando para solucionar el error`)
	}

	return email.EntityEmailOtpResponse{
		Response: "Correo enviado exitosamente a " + data.Email,
		Details: struct {
			ServiceExternal          string `json:"service_external"`
			ResponseServicesExternal string `json:"responses_ervices_external"`
		}{
			ServiceExternal:          "Brevo API v3",
			ResponseServicesExternal: result.MessageId, // ID único del mensaje en Brevo
		},
	}, nil
}
