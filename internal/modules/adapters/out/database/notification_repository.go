package database

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/streamingNotifyHub/internal/modules/domains/entities/notification"
)

type NotificationRepository struct {
	pool *pgxpool.Pool
}

func (r *NotificationRepository) ListInbox(ctx context.Context, userID string, limit int, cursor string) (notification.InboxPage, error) {
	if limit < 1 {
		limit = 30
	}
	if limit > 100 {
		limit = 100
	}
	var cursorTime *time.Time
	var cursorID *string
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return notification.InboxPage{}, fmt.Errorf("cursor invalido")
		}
		parts := strings.SplitN(string(decoded), "|", 2)
		if len(parts) != 2 {
			return notification.InboxPage{}, fmt.Errorf("cursor invalido")
		}
		parsed, err := time.Parse(time.RFC3339Nano, parts[0])
		if err != nil {
			return notification.InboxPage{}, fmt.Errorf("cursor invalido")
		}
		cursorTime, cursorID = &parsed, &parts[1]
	}
	const query = `
		SELECT n.notification_id, n.user_id, n.category_code, n.title, COALESCE(n.body, ''),
		       COALESCE(n.action_url, ''), COALESCE(n.actor_user_id::text, ''), actor.avatar_url,
		       n.read_at, n.created_at
		FROM ecosystem_core_auth.notifications n
		LEFT JOIN ecosystem_core_auth.user_profile actor ON actor.user_id = n.actor_user_id
		WHERE n.user_id = $1
		  AND (n.expires_at IS NULL OR n.expires_at > NOW())
		  AND ($2::timestamptz IS NULL OR (n.created_at, n.notification_id) < ($2, $3::uuid))
		ORDER BY n.created_at DESC, n.notification_id DESC
		LIMIT $4`
	rows, err := r.pool.Query(ctx, query, userID, cursorTime, cursorID, limit+1)
	if err != nil {
		return notification.InboxPage{}, fmt.Errorf("consultando bandeja: %w", err)
	}
	defer rows.Close()
	items := make([]notification.InboxItem, 0, limit+1)
	for rows.Next() {
		var item notification.InboxItem
		var readAt *time.Time
		var createdAt time.Time
		if err := rows.Scan(&item.NotificationID, &item.UserID, &item.CategoryCode, &item.Title, &item.Body, &item.ActionURL, &item.ActorUserID, &item.ActorAvatarURL, &readAt, &createdAt); err != nil {
			return notification.InboxPage{}, err
		}
		item.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
		if readAt != nil {
			value := readAt.UTC().Format(time.RFC3339Nano)
			item.ReadAt = &value
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return notification.InboxPage{}, err
	}
	page := notification.InboxPage{Items: items, HasMore: len(items) > limit}
	if page.HasMore {
		page.Items = items[:limit]
	}
	if len(page.Items) > 0 {
		last := page.Items[len(page.Items)-1]
		parsed, _ := time.Parse(time.RFC3339Nano, last.CreatedAt)
		value := base64.RawURLEncoding.EncodeToString([]byte(parsed.UTC().Format(time.RFC3339Nano) + "|" + last.NotificationID))
		page.NextCursor = &value
	}
	return page, nil
}

func (r *NotificationRepository) UnreadCount(ctx context.Context, userID string) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM ecosystem_core_auth.notifications WHERE user_id = $1 AND read_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())`, userID).Scan(&count)
	return count, err
}

func NewNotificationRepository(pool *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{pool: pool}
}

func (r *NotificationRepository) FindCategory(ctx context.Context, code string) (notification.Category, bool, error) {
	const query = `
		SELECT category_code, allows_in_app, allows_email, allows_push, is_mandatory, is_active
		FROM ecosystem_core_auth.notification_categories
		WHERE category_code = $1 AND is_active = true`

	var category notification.Category
	err := r.pool.QueryRow(ctx, query, code).Scan(
		&category.Code,
		&category.AllowsInApp,
		&category.AllowsEmail,
		&category.AllowsPush,
		&category.IsMandatory,
		&category.IsActive,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return notification.Category{}, false, nil
	}
	if err != nil {
		return notification.Category{}, false, fmt.Errorf("consultando categoria %q: %w", code, err)
	}

	return category, true, nil
}

func (r *NotificationRepository) FindUserSettings(ctx context.Context, userID string) (notification.UserSettings, error) {
	const query = `
		SELECT email_notifications, push_notifications
		FROM ecosystem_core_auth.user_notification_settings
		WHERE user_id = $1`

	settings := notification.UserSettings{UserID: userID}
	err := r.pool.QueryRow(ctx, query, userID).Scan(
		&settings.EmailNotifications,
		&settings.PushNotifications,
	)

	// Un usuario que nunca abrio los ajustes no tiene fila. Eso no es un fallo:
	// se aplican los mismos valores por defecto que declara la tabla.
	if errors.Is(err, pgx.ErrNoRows) {
		settings.EmailNotifications = true
		settings.PushNotifications = true
		return settings, nil
	}
	if err != nil {
		return notification.UserSettings{}, fmt.Errorf("consultando preferencias de %q: %w", userID, err)
	}

	return settings, nil
}

func (r *NotificationRepository) Save(
	ctx context.Context,
	item notification.Notification,
	deliveries []notification.Delivery,
) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("abriendo transaccion: %w", err)
	}
	// Si algo falla entre medias, el Rollback deshace la notificacion y sus
	// entregas juntas. Una notificacion sin entregas seria invisible para el
	// worker de envio y no la recuperaria nadie.
	defer tx.Rollback(ctx)

	const insertNotification = `
		INSERT INTO ecosystem_core_auth.notifications (
			user_id, category_code, source_service, event_type, event_id,
			topic, routing_key, title, body, action_url, actor_user_id,
			payload, status, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (source_service, event_id) DO NOTHING
		RETURNING notification_id`

	var notificationID string
	err = tx.QueryRow(ctx, insertNotification,
		item.UserID,
		item.CategoryCode,
		item.SourceService,
		item.EventType,
		item.EventID,
		item.Topic,
		nullable(item.RoutingKey),
		item.Title,
		nullable(item.Body),
		nullable(item.ActionURL),
		nullable(item.ActorUserID),
		item.Payload,
		item.Status,
		item.ExpiresAt,
	).Scan(&notificationID)

	// Sin filas devueltas significa que el ON CONFLICT salto: este evento ya
	// estaba guardado. Es el camino normal de un reintento, no un error.
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insertando notificacion: %w", err)
	}

	const insertDelivery = `
		INSERT INTO ecosystem_core_auth.notification_deliveries (
			notification_id, channel, terminal, status
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (notification_id, channel) DO NOTHING`

	batch := &pgx.Batch{}
	for _, delivery := range deliveries {
		batch.Queue(insertDelivery, notificationID, delivery.Channel, delivery.Terminal, delivery.Status)
	}

	results := tx.SendBatch(ctx, batch)
	for range deliveries {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return false, fmt.Errorf("insertando entregas: %w", err)
		}
	}
	// El batch se cierra antes del Commit: mientras siga abierto la conexion
	// esta ocupada y el Commit falla.
	if err := results.Close(); err != nil {
		return false, fmt.Errorf("cerrando batch de entregas: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("confirmando transaccion: %w", err)
	}

	return true, nil
}

// nullable convierte la cadena vacia en NULL. Las columnas opcionales se
// consultan con `IS NULL`; guardar "" haria que una fila sin `action_url`
// pareciera tener una vacia.
func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
