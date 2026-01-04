package emails

import (
	"bytes"
	"errors"
	"text/template"

	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/email"
	"github.com/streamingNotifyHub/internal/modules/domains/ports"
)

type SendOtpEmaisUseCase struct {
	RepositoryEmails ports.EmailsRepositoryInterface
}

func NewSendOtpEmaisUseCase(repo ports.EmailsRepositoryInterface) *SendOtpEmaisUseCase {
	return &SendOtpEmaisUseCase{
		RepositoryEmails: repo,
	}
}

func (uc *SendOtpEmaisUseCase) ExecuteSendEmailsUseCase(data command.SendOtpCommandRequest) (email.EntityEmailOtpResponse, error) {
	_, errValid := email.NewEntityOtp(data)

	if errValid != nil {
		// Retornamos un error conocido (401 o 422 según prefieras)
		return email.EntityEmailOtpResponse{}, config.NewErrCodeEntitiesDataInvalid(errors.New(errValid.Error()))
	}
	// 1. Datos para el template
	dataTemplated := map[string]interface{}{
		"codeOtp":  data.Code,
		"timeCode": data.TimeCodeVerification,
	}

	tmpl, err := template.ParseFiles("internal/modules/application/templates/SendOtpsEmail.html")
	if err != nil {
		return email.EntityEmailOtpResponse{}, config.NewInternalServerError(err)
	}

	var tplBuffer bytes.Buffer
	if err := tmpl.Execute(&tplBuffer, dataTemplated); err != nil {
		return email.EntityEmailOtpResponse{}, config.NewErrCodeBadRequestDataSystem(err)
	}

	html := tplBuffer.String()

	// 5. Enviar al repositorio
	response, errRepository := uc.RepositoryEmails.SendOtpsEmails(data, html)
	if errRepository != nil {
		return email.EntityEmailOtpResponse{}, config.NewErrCodeBadRequestDataSystem(errRepository)
	}

	return response, nil
}
