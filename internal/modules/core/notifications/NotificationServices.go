package notifications

import (
	"bytes"
	"fmt"
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
	code := data.Code
	if code == "" {
		return email.EntityEmailOtpResponse{}, config.NewErrCodeEntitiesDataInvalid(fmt.Errorf("código OTP no configurado"))
	}

	// La plantilla imprime este valor tal cual detrás de "Caduca en": sin la
	// unidad quedaba "Caduca en 5.".
	dataTemplated := map[string]string{
		"codeOtp":  code,
		"timeCode": "5 minutos",
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

	cacheKey := fmt.Sprintf("%s:%s", constants.REDIS_KEYS[0], data.Email)
	cacheValue := map[string]string{"code": code}
	if data.TypeTemplated == "login_verification" && data.ChallengeID != "" {
		cacheKey = fmt.Sprintf("auth:login_email_otp:%s", data.ChallengeID)
		// Rust deserializa este valor como LoginEmailOtp. No se puede guardar
		// solamente el codigo porque se perderia el identificador del desafio.
		cacheValue["challenge_id"] = data.ChallengeID
	}
	config.SetCache(cacheKey, cacheValue, 5*time.Minute)

	return response, nil
}
