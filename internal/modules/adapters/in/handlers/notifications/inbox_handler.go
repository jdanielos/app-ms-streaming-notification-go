package notifications

import (
	"context"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/streamingNotifyHub/internal/modules/domains/ports"
)

type InboxHandler struct {
	repository ports.NotificationRepositoryInterface
}

func NewInboxHandler(repository ports.NotificationRepositoryInterface) *InboxHandler {
	return &InboxHandler{repository: repository}
}

func (h *InboxHandler) GetInbox(c *fiber.Ctx) error {
	userID := c.Get("x-account-id")
	if userID == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "x-account-id requerido"})
	}
	limit, _ := strconv.Atoi(c.Query("limit", "30"))
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
	defer cancel()
	page, err := h.repository.ListInbox(ctx, userID, limit, c.Query("cursor"))
	if err != nil {
		return err
	}
	count, err := h.repository.UnreadCount(ctx, userID)
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{"data": page, "unreadCount": count})
}
