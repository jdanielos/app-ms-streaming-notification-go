package emails

import (
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	"github.com/streamingNotifyHub/internal/modules/application/emails"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/command"
)

type SendOtpEmaisHandler struct {
	Usecase *emails.SendOtpEmaisUseCase
}

func NewSendOtpEmaisHandler(usc *emails.SendOtpEmaisUseCase) *SendOtpEmaisHandler {
	return &SendOtpEmaisHandler{
		Usecase: usc,
	}
}

func (uc *SendOtpEmaisHandler) ExecuteSendEmailsHandlers(c *fiber.Ctx) error {

	var data command.SendOtpCommandRequest

	// 1. Recibimos JSON -> Command
	if err := c.BodyParser(&data); err != nil {
		slog.Warn("error: " + err.Error())
		return config.NewUnprStatusUnprocessableEntity(err)
	}
	response, errRepository := uc.Usecase.ExecuteSendEmailsUseCase(data)

	if errRepository != nil {
		slog.Error("error: " + errRepository.Error())
		return config.NewUnprStatusUnprocessableEntity(errRepository)
	}

	return config.SendOk(c, response, "Opt sucessfull")
}
