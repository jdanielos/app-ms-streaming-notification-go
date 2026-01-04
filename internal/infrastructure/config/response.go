package config

import "github.com/gofiber/fiber/v2"

func SendOk(c *fiber.Ctx, data interface{}, message string) error {
	return c.Status(fiber.StatusOK).JSON(ResponseSystem{
		Success:      true,
		Data:         data,
		Message:      message,
		Code:         0,
		InternalCode: "SUCCESS",
		Status:       200,
	})
}
