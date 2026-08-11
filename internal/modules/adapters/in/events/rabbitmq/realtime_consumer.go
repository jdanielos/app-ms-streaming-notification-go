package events

import (
	"encoding/json"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
	"github.com/streamingNotifyHub/internal/infrastructure/constants"
	"github.com/streamingNotifyHub/internal/infrastructure/realtime"
)

type RealtimeCommentEventConsumer struct {
	channel *amqp091.Channel
	hub     *realtime.NotificationHub
}

func NewRealtimeCommentEventConsumer(channel *amqp091.Channel, hub *realtime.NotificationHub) *RealtimeCommentEventConsumer {
	return &RealtimeCommentEventConsumer{channel: channel, hub: hub}
}

func (c *RealtimeCommentEventConsumer) Start() {
	if c.channel == nil {
		slog.Error("realtime_consumer_without_rabbit_channel")
		return
	}
	if err := c.channel.ExchangeDeclare(constants.REALTIME_WEBSOCKET_EXCHANGE, "topic", true, false, false, false, nil); err != nil {
		slog.Error("realtime_exchange_declare_failed", "error", err)
		return
	}
	if _, err := c.channel.QueueDeclare(constants.REALTIME_WEBSOCKET_QUEUE, true, false, false, false, nil); err != nil {
		slog.Error("realtime_queue_declare_failed", "error", err)
		return
	}
	if err := c.channel.QueueBind(constants.REALTIME_WEBSOCKET_QUEUE, "video.*.comment.*", constants.REALTIME_WEBSOCKET_EXCHANGE, false, nil); err != nil {
		slog.Error("realtime_queue_bind_failed", "error", err)
		return
	}
	messages, err := c.channel.Consume(constants.REALTIME_WEBSOCKET_QUEUE, "notify-hub-realtime-comments", false, false, false, false, nil)
	if err != nil {
		slog.Error("realtime_consumer_register_failed", "error", err)
		return
	}
	go func() {
		for message := range messages {
			var event realtime.CommentEvent
			if err := json.Unmarshal(message.Body, &event); err != nil {
				slog.Error("realtime_event_decode_failed", "error", err)
				_ = message.Nack(false, false)
				continue
			}
			c.hub.PublishComment(event)
			_ = message.Ack(false)
		}
	}()
	slog.Info("realtime_comment_consumer_started", "exchange", constants.REALTIME_WEBSOCKET_EXCHANGE, "queue", constants.REALTIME_WEBSOCKET_QUEUE)
}
