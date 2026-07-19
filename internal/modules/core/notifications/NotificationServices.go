package notifications

import (
	"bytes"
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

type NotificationServices struct {
	ports ports.EventEmailsInterface
}

func NewSendClientOtpEmaisUseCase(ports ports.EventEmailsInterface) *NotificationServices {
	return &NotificationServices{
		ports: ports,
	}
}

func (uc *NotificationServices) SendOtpService(data *command.AuthenticatedUserCommand) (email.EntityEmailOtpResponse, error) {
	code := rand.Intn(1000000)

	dataTemplated := map[string]int{
		"codeOtp":  code,
		"timeCode": 5,
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
	response, errRepository := uc.ports.SendOtpsEmail(data, html)
	if errRepository != nil {
		return email.EntityEmailOtpResponse{}, config.NewErrCodeBadRequestDataSystem(errRepository)
	}

	config.SetCache(fmt.Sprintf("%s:%s", constants.REDIS_KEYS[0], data.Email), map[string]int{"code": code}, 5*time.Minute)

	return response, nil
}
