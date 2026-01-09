package emails

import (
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/streamingNotifyHub/internal/infrastructure/config"

	"github.com/streamingNotifyHub/internal/modules/core/emails"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
)

type SendClientOtpEmaisHandler struct {
	Usecase *emails.SendClientOtpEmaisUseCase
}

func NewSendClientOtpEmaisHandler(usc *emails.SendClientOtpEmaisUseCase) *SendClientOtpEmaisHandler {
	return &SendClientOtpEmaisHandler{
		Usecase: usc,
	}
}

func (uc *SendClientOtpEmaisHandler) ExecuteSendClientEmailsHandlers(c *fiber.Ctx) error {

	var data command.SendOtpCommandRequest

	// 1. Recibimos JSON -> Command
	if err := c.BodyParser(&data); err != nil {
		slog.Warn("error: " + err.Error())
		return config.NewUnprStatusUnprocessableEntity(err)
	}
	response, errRepository := uc.Usecase.ExecuteSendClientEmailsUseCase(data)

	if errRepository != nil {
		slog.Error("error: " + errRepository.Error())
		return config.NewUnprStatusUnprocessableEntity(errRepository)
	}

	return config.SendOk(c, response, "Opt sucessfull")
}
