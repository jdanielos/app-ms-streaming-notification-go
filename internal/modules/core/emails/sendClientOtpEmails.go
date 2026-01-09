package emails

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"text/template"
	"time"

	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/email"
	"github.com/streamingNotifyHub/internal/modules/domains/ports"
)

type SendClientOtpEmaisUseCase struct {
	RepositoryEmails ports.EmailsRepositoryInterface
}

func NewSendClientOtpEmaisUseCase(repo ports.EmailsRepositoryInterface) *SendClientOtpEmaisUseCase {
	return &SendClientOtpEmaisUseCase{
		RepositoryEmails: repo,
	}
}

func (uc *SendClientOtpEmaisUseCase) ExecuteSendClientEmailsUseCase(data command.SendOtpCommandRequest) (email.EntityEmailOtpResponse, error) {
	_, errValid := email.NewEntityOtp(data)

	if errValid != nil {
		return email.EntityEmailOtpResponse{}, config.NewErrCodeEntitiesDataInvalid(errors.New(errValid.Error()))
	}
	code := fmt.Sprintf("%06d", rand.Intn(1000000))

	data.Subject = fmt.Sprintf("Tu codigo es: %s", code)

	dataTemplated := map[string]interface{}{
		"codeOtp":  code,
		"timeCode": data.TimeCodeVerification,
	}

	tmpl, err := template.ParseFiles("internal/modules/core/templates/SendOtpsEmail.html")
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

	config.SetCache(fmt.Sprintf("%s:%s", constants.REDIS_KEYS[0], data.Email), map[string]string{"code": code}, 5*time.Minute)

	return response, nil
}
