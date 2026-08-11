package realtime

import (
	"encoding/json"
	"log/slog"
	"strings"
	"sync"

	"github.com/gofiber/contrib/websocket"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/notification"
)

type wireMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type initPayload struct {
	Headers map[string]string `json:"headers"`
}

type subscribePayload struct {
	Query     string          `json:"query"`
	Variables json.RawMessage `json:"variables"`
}

type typingMessage struct {
	VideoID    string       `json:"videoId"`
	TypingUser *commentUser `json:"typingUser"`
}

type notificationMessage struct {
	NotificationID string  `json:"notificationId"`
	UserID         string  `json:"userId"`
	CategoryCode   string  `json:"categoryCode"`
	Title          string  `json:"title"`
	Body           string  `json:"body,omitempty"`
	ActionURL      string  `json:"actionUrl,omitempty"`
	ActorUserID    string  `json:"actorUserId,omitempty"`
	CreatedAt      string  `json:"createdAt"`
	ReadAt         *string `json:"readAt"`
}

type commentUser struct {
	UserID      string  `json:"userId"`
	Username    string  `json:"username"`
	DisplayName *string `json:"displayName"`
	AvatarURL   *string `json:"avatarUrl"`
}

type comment struct {
	ID                string  `json:"id"`
	VideoID           string  `json:"videoId"`
	UserID            string  `json:"userId"`
	ParentID          *string `json:"parentId"`
	Content           string  `json:"content"`
	LikesCount        int64   `json:"likesCount"`
	CreatedAt         string  `json:"createdAt"`
	Username          string  `json:"username"`
	DisplayName       *string `json:"displayName"`
	AvatarURL         *string `json:"avatarUrl"`
	ParentUsername    *string `json:"parentUsername"`
	ParentDisplayName *string `json:"parentDisplayName"`
	ParentAvatarURL   *string `json:"parentAvatarUrl"`
	IsMine            bool    `json:"isMine"`
	IsLikedByMe       bool    `json:"isLikedByMe"`
	RepliesCount      int64   `json:"repliesCount"`
}

type CommentEvent struct {
	VideoID    string       `json:"videoId"`
	ParentID   *string      `json:"parentId"`
	Kind       string       `json:"kind"`
	Comment    *comment     `json:"comment"`
	TypingUser *commentUser `json:"typingUser"`
}

type commentEventPayload struct {
	VideoID    string       `json:"videoId"`
	ParentID   *string      `json:"parentId"`
	Kind       string       `json:"kind"`
	Comment    *comment     `json:"comment"`
	TypingUser *commentUser `json:"typingUser"`
}

type subscription struct{ kind, videoID string }

type client struct {
	conn          *websocket.Conn
	mu            sync.Mutex
	subscriptions map[string]subscription
}

// NotificationHub es el unico gateway WebSocket. Un socket puede tener varias
// suscripciones: notificaciones por usuario y eventos de comentarios por video.
type NotificationHub struct {
	mu      sync.RWMutex
	clients map[string]map[*client]struct{}
}

func NewNotificationHub() *NotificationHub {
	return &NotificationHub{clients: make(map[string]map[*client]struct{})}
}

func (h *NotificationHub) Handle(conn *websocket.Conn) {
	var accountID string
	var current *client
	defer func() {
		if current != nil {
			h.remove(accountID, current)
		}
	}()

	for {
		var message wireMessage
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		switch message.Type {
		case "connection_init":
			var payload initPayload
			if err := json.Unmarshal(message.Payload, &payload); err != nil {
				return
			}
			accountID = payload.Headers["x-account-id"]
			if accountID == "" {
				_ = conn.WriteJSON(map[string]any{"type": "connection_error", "payload": map[string]string{"message": "x-account-id requerido"}})
				return
			}
			current = &client{conn: conn, subscriptions: make(map[string]subscription)}
			h.add(accountID, current)
			slog.Info("notification_websocket_authenticated", "user_id", accountID)
			if err := conn.WriteJSON(map[string]string{"type": "connection_ack"}); err != nil {
				return
			}
		case "subscribe":
			if current == nil || message.ID == "" {
				return
			}
			var payload subscribePayload
			if err := json.Unmarshal(message.Payload, &payload); err != nil {
				return
			}
			stream := subscription{kind: "notification"}
			if strings.Contains(payload.Query, "videoCommentEvents") {
				var vars struct {
					VideoID string `json:"videoId"`
				}
				if err := json.Unmarshal(payload.Variables, &vars); err != nil || vars.VideoID == "" {
					return
				}
				stream = subscription{kind: "comments", videoID: vars.VideoID}
			}
			current.mu.Lock()
			current.subscriptions[message.ID] = stream
			current.mu.Unlock()
			slog.Info("notification_websocket_subscribed", "user_id", accountID, "subscription_id", message.ID, "stream", stream.kind, "video_id", stream.videoID)
		case "typing":
			if current == nil || accountID == "" {
				return
			}
			var payload typingMessage
			if err := json.Unmarshal(message.Payload, &payload); err != nil || payload.VideoID == "" {
				continue
			}
			if payload.TypingUser == nil {
				payload.TypingUser = &commentUser{}
			}
			// La identidad viene de connection_init; el resto es metadata visual.
			payload.TypingUser.UserID = accountID
			slog.Debug("comment_websocket_typing_received", "user_id", accountID, "video_id", payload.VideoID)
			h.PublishComment(CommentEvent{VideoID: payload.VideoID, Kind: "typing", TypingUser: payload.TypingUser})
		case "complete":
			if current != nil {
				current.mu.Lock()
				delete(current.subscriptions, message.ID)
				current.mu.Unlock()
			}
		}
	}
}

func (h *NotificationHub) Publish(item notification.Notification) {
	h.mu.RLock()
	users := make([]*client, 0, len(h.clients[item.UserID]))
	for c := range h.clients[item.UserID] {
		users = append(users, c)
	}
	h.mu.RUnlock()
	payload := notificationMessage{NotificationID: item.EventID, UserID: item.UserID, CategoryCode: item.CategoryCode, Title: item.Title, Body: item.Body, ActionURL: item.ActionURL, ActorUserID: item.ActorUserID, CreatedAt: item.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999Z07:00")}
	for _, c := range users {
		c.writeStream("notificationReceived", payload, func(s subscription) bool { return s.kind == "notification" })
	}
	slog.Info("notification_websocket_publish", "user_id", item.UserID, "recipients", len(users), "event_id", item.EventID)
}

func (h *NotificationHub) PublishComment(event CommentEvent) {
	h.mu.RLock()
	recipients := make([]*client, 0)
	for _, clients := range h.clients {
		for c := range clients {
			recipients = append(recipients, c)
		}
	}
	h.mu.RUnlock()
	payload := commentEventPayload{VideoID: event.VideoID, ParentID: event.ParentID, Kind: event.Kind, Comment: event.Comment, TypingUser: event.TypingUser}
	for _, c := range recipients {
		c.writeStream("videoCommentEvents", payload, func(s subscription) bool { return s.kind == "comments" && strings.EqualFold(s.videoID, event.VideoID) })
	}
	slog.Info("comment_websocket_publish", "video_id", event.VideoID, "kind", event.Kind, "recipients", len(recipients))
}

func (c *client) writeStream(field string, data any, matches func(subscription) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, stream := range c.subscriptions {
		if !matches(stream) {
			continue
		}
		message := map[string]any{"id": id, "type": "next", "payload": map[string]any{"data": map[string]any{field: data}}}
		if err := c.conn.WriteJSON(message); err != nil {
			slog.Debug("websocket_write_failed", "error", err)
			return
		}
	}
}

func (h *NotificationHub) add(accountID string, c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[accountID] == nil {
		h.clients[accountID] = make(map[*client]struct{})
	}
	h.clients[accountID][c] = struct{}{}
}
func (h *NotificationHub) remove(accountID string, c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients[accountID], c)
	if len(h.clients[accountID]) == 0 {
		delete(h.clients, accountID)
	}
}
