package notifications

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/streamingNotifyHub/internal/infrastructure/config"
	dto "github.com/streamingNotifyHub/internal/modules/adapters/in/events/Dto"
)

const (
	notificationDebounceWindow = time.Minute
	notificationDebounceDueKey = "notify:debounce:due"
)

type pendingNotification struct {
	Event       dto.NotificationEventDTO `json:"event"`
	RoutingKey  string                   `json:"routing_key"`
	Scope       string                   `json:"scope"`
	Count       int                      `json:"count"`
	ActorUserID []string                 `json:"actor_user_ids"`
	StartedAt   int64                    `json:"started_at"`
}

// NotificationDebouncer deja los eventos de campana en Redis durante una
// ventana de inactividad. El comentario real no pasa por aqui: ya fue guardado
// por Multimedia y enviado en tiempo real a quienes ven el video.
type NotificationDebouncer struct {
	redis   *redis.Client
	usecase *ProcessNotificationUseCase
}

func NewNotificationDebouncer(usecase *ProcessNotificationUseCase) *NotificationDebouncer {
	return &NotificationDebouncer{
		redis:   config.GetRedisClient(os.Getenv("URL_REDIS_SYSTEM"), 3),
		usecase: usecase,
	}
}

func (d *NotificationDebouncer) Enqueue(event dto.NotificationEventDTO, routingKey string) (bool, error) {
	if event.CategoryCode != "comment" && event.CategoryCode != "like" {
		return false, nil
	}

	scope := notificationScope(event)
	key := notificationDebounceKey(event.UserID, event.CategoryCode, scope)
	ctx := context.Background()
	var pending pendingNotification
	if raw, err := d.redis.Get(ctx, key).Bytes(); err == nil {
		if err := json.Unmarshal(raw, &pending); err != nil {
			return false, err
		}
	} else if err != redis.Nil {
		return false, err
	} else {
		pending = pendingNotification{Event: event, RoutingKey: routingKey, Scope: scope, StartedAt: time.Now().UTC().UnixMilli()}
	}

	pending.Count++
	pending.Event = event
	pending.RoutingKey = routingKey
	if event.ActorUserID != "" && !contains(pending.ActorUserID, event.ActorUserID) {
		pending.ActorUserID = append(pending.ActorUserID, event.ActorUserID)
	}

	payload, err := json.Marshal(pending)
	if err != nil {
		return false, err
	}
	dueAt := time.Now().Add(notificationDebounceWindow).UnixMilli()
	pipe := d.redis.TxPipeline()
	pipe.Set(ctx, key, payload, 10*time.Minute)
	pipe.ZAdd(ctx, notificationDebounceDueKey, &redis.Z{Score: float64(dueAt), Member: key})
	_, err = pipe.Exec(ctx)
	return true, err
}

func (d *NotificationDebouncer) Start() {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			d.flushDue(context.Background())
		}
	}()
}

func (d *NotificationDebouncer) flushDue(ctx context.Context) {
	keys, err := d.redis.ZRangeByScore(ctx, notificationDebounceDueKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().UnixMilli()), Offset: 0, Count: 100}).Result()
	if err != nil {
		slog.Error("notification_debounce_due_lookup_failed", "error", err)
		return
	}
	for _, key := range keys {
		lockKey := key + ":lock"
		locked, err := d.redis.SetNX(ctx, lockKey, "1", 15*time.Second).Result()
		if err != nil || !locked {
			continue
		}
		d.flushOne(ctx, key)
		_ = d.redis.Del(ctx, lockKey).Err()
	}
}

func (d *NotificationDebouncer) flushOne(ctx context.Context, key string) {
	raw, err := d.redis.Get(ctx, key).Bytes()
	if err == redis.Nil {
		_ = d.redis.ZRem(ctx, notificationDebounceDueKey, key).Err()
		return
	}
	if err != nil {
		return
	}
	var pending pendingNotification
	if err := json.Unmarshal(raw, &pending); err != nil {
		slog.Error("notification_debounce_decode_failed", "error", err)
		_ = d.redis.Del(ctx, key).Err()
		_ = d.redis.ZRem(ctx, notificationDebounceDueKey, key).Err()
		return
	}

	event := pending.Event
	event.EventID = fmt.Sprintf("digest:%s:%d", key, pending.StartedAt)
	event.EventType += ".digest"
	event.Title = digestTitle(event.CategoryCode, pending.Count)
	metadata := map[string]any{"count": pending.Count, "actor_user_ids": pending.ActorUserID, "scope": pending.Scope}
	if original := event.RawPayload(); len(original) > 0 {
		metadata["last_event"] = json.RawMessage(original)
	}
	event.Payload, _ = json.Marshal(metadata)

	if _, err := d.usecase.Execute(ctx, event, pending.RoutingKey); err != nil {
		slog.Error("notification_debounce_flush_failed", "error", err, "user_id", event.UserID)
		_ = d.redis.ZAdd(ctx, notificationDebounceDueKey, &redis.Z{Score: float64(time.Now().Add(5 * time.Second).UnixMilli()), Member: key}).Err()
		return
	}
	pipe := d.redis.TxPipeline()
	pipe.Del(ctx, key)
	pipe.ZRem(ctx, notificationDebounceDueKey, key)
	_, _ = pipe.Exec(ctx)
}

func notificationScope(event dto.NotificationEventDTO) string {
	var payload map[string]any
	_ = json.Unmarshal(event.RawPayload(), &payload)
	if event.CategoryCode == "like" {
		if id, ok := payload["comment_id"].(string); ok && id != "" {
			return id
		}
	}
	if id, ok := payload["video_id"].(string); ok && id != "" {
		return id
	}
	return event.EventType
}

func notificationDebounceKey(userID, category, scope string) string {
	hash := sha1.Sum([]byte(userID + ":" + category + ":" + scope))
	return "notify:debounce:data:" + hex.EncodeToString(hash[:])
}

func contains(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func digestTitle(category string, count int) string {
	if category == "like" {
		if count == 1 {
			return "Nuevo me gusta"
		}
		return fmt.Sprintf("%d nuevos me gusta", count)
	}
	if count == 1 {
		return "Nuevo comentario"
	}
	return fmt.Sprintf("%d nuevos comentarios", count)
}
