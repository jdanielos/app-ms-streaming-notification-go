package ports

import "github.com/streamingNotifyHub/internal/modules/domains/entities/notification"

type NotificationPublisher interface {
	Publish(notification.Notification)
}
