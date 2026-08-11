-- =============================================================================
-- Hub de notificaciones — esquema ecosystem_core_auth (DATABASE_USERS_URL)
--
-- Convive con `user_notification_settings`, que ya existe y guarda QUE quiere
-- recibir el usuario. Esto guarda QUE se le envio, POR DONDE y COMO acabo.
--
-- Se ejecuta en orden. No borra nada de lo que ya hay.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Catalogo de categorias
--
-- `user_notification_settings` decide por canal (email / push) con booleanos
-- fijos. Eso no distingue "un comentario nuevo" de "alguien entro a tu cuenta",
-- y son cosas muy distintas: la segunda hay que entregarla aunque el usuario
-- tenga todo apagado. La categoria es la que aporta ese matiz.
-- -----------------------------------------------------------------------------
CREATE TABLE ecosystem_core_auth.notification_categories (
    category_code   varchar(64)  NOT NULL,
    display_name_es varchar(120) NOT NULL,
    display_name_en varchar(120) NOT NULL,
    description     text         NULL,

    -- Canales que la categoria admite. Un "empezo un directo" no tiene sentido
    -- por correo: llega tarde y molesta.
    allows_in_app   boolean      NOT NULL DEFAULT true,
    allows_email    boolean      NOT NULL DEFAULT false,
    allows_push     boolean      NOT NULL DEFAULT false,

    -- Obligatoria: ignora las preferencias del usuario. Reservado a seguridad
    -- de la cuenta (OTP, acceso desde dispositivo nuevo, cambio de contraseña).
    -- Sin esta bandera, alguien que apaga las notificaciones se queda sin recibir
    -- el aviso de que le entraron a la cuenta.
    is_mandatory    boolean      NOT NULL DEFAULT false,

    is_active       boolean      NOT NULL DEFAULT true,
    created_at      timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT notification_categories_pkey PRIMARY KEY (category_code)
);

COMMENT ON TABLE  ecosystem_core_auth.notification_categories IS 'Catalogo de tipos de notificacion y los canales que admite cada uno.';
COMMENT ON COLUMN ecosystem_core_auth.notification_categories.is_mandatory IS 'Si es true se entrega aunque el usuario tenga el canal apagado. Solo para seguridad de la cuenta.';


-- -----------------------------------------------------------------------------
-- 2. Notificaciones recibidas
--
-- Una fila por mensaje consumido del exchange, ya resuelto a un usuario.
-- -----------------------------------------------------------------------------
CREATE TABLE ecosystem_core_auth.notifications (
    notification_id uuid         NOT NULL DEFAULT gen_random_uuid(),
    user_id         uuid         NOT NULL,
    category_code   varchar(64)  NOT NULL,

    -- Trazabilidad del origen -------------------------------------------------
    source_service  varchar(64)  NOT NULL,   -- users | multimedia | streaming | notify
    event_type      varchar(96)  NOT NULL,   -- video.published, comment.created, auth.otp
    -- Identificador del evento en el servicio que lo publico. RabbitMQ entrega
    -- AL MENOS UNA VEZ: sin esta clave, un reintento tras un fallo de red crea
    -- la misma notificacion dos veces. Es el unico seguro contra duplicados.
    event_id        varchar(128) NOT NULL,

    -- Canal por el que viajo --------------------------------------------------
    -- Direccion completa tal como se publico, en el formato acordado:
    --   mqtt://{topic}/message/{user_id}/{category_code}
    topic           text         NOT NULL,
    -- La routing key de AMQP, aparte. Sirve para reproducir el mensaje contra el
    -- exchange sin tener que deducirla del topic.
    routing_key     varchar(255) NULL,

    -- Contenido ---------------------------------------------------------------
    title           varchar(180) NOT NULL,
    body            text         NULL,
    action_url      text         NULL,       -- a donde lleva al tocarla
    actor_user_id   uuid         NULL,       -- quien la provoco, si fue alguien
    -- Carga original completa. Permite reconstruir o reprocesar la notificacion
    -- sin volver a pedirle nada al servicio que la emitio.
    payload         jsonb        NOT NULL DEFAULT '{}'::jsonb,

    -- Estado ------------------------------------------------------------------
    status          varchar(16)  NOT NULL DEFAULT 'pending',
    read_at         timestamptz  NULL,
    -- Caducidad. "Fulano esta en vivo" no sirve mañana; sin esto la bandeja se
    -- llena de avisos que ya no significan nada.
    expires_at      timestamptz  NULL,

    created_at      timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT notifications_pkey PRIMARY KEY (notification_id),
    CONSTRAINT notifications_status_check CHECK (status IN ('pending', 'processed', 'failed', 'discarded')),
    -- Idempotencia: el mismo evento del mismo servicio nunca entra dos veces.
    CONSTRAINT notifications_event_uniq UNIQUE (source_service, event_id)
);

COMMENT ON TABLE  ecosystem_core_auth.notifications IS 'Notificaciones consumidas del bus, ya resueltas a un usuario.';
COMMENT ON COLUMN ecosystem_core_auth.notifications.event_id IS 'Id del evento en el servicio origen. Clave de idempotencia frente a la entrega at-least-once de RabbitMQ.';
COMMENT ON COLUMN ecosystem_core_auth.notifications.topic IS 'Direccion completa de publicacion: mqtt://{topic}/message/{user_id}/{category_code}';
COMMENT ON COLUMN ecosystem_core_auth.notifications.payload IS 'Mensaje original sin transformar, para reprocesar sin volver al servicio origen.';

ALTER TABLE ecosystem_core_auth.notifications
    ADD CONSTRAINT notifications_user_fkey
    FOREIGN KEY (user_id) REFERENCES ecosystem_core_auth.users(user_id) ON DELETE CASCADE;

-- El actor no se borra en cascada: si se da de baja quien comento, la
-- notificacion sigue teniendo sentido, solo se queda sin autor.
ALTER TABLE ecosystem_core_auth.notifications
    ADD CONSTRAINT notifications_actor_fkey
    FOREIGN KEY (actor_user_id) REFERENCES ecosystem_core_auth.users(user_id) ON DELETE SET NULL;

ALTER TABLE ecosystem_core_auth.notifications
    ADD CONSTRAINT notifications_category_fkey
    FOREIGN KEY (category_code) REFERENCES ecosystem_core_auth.notification_categories(category_code);

-- La consulta de la bandeja: las del usuario, mas recientes primero.
CREATE INDEX notifications_user_created_idx
    ON ecosystem_core_auth.notifications (user_id, created_at DESC);

-- Contador de no leidas. Parcial: solo indexa las que no se han leido, que son
-- una minoria, asi que el indice se mantiene pequeño con el tiempo.
CREATE INDEX notifications_unread_idx
    ON ecosystem_core_auth.notifications (user_id)
    WHERE read_at IS NULL;

-- Barrido de caducadas.
CREATE INDEX notifications_expires_idx
    ON ecosystem_core_auth.notifications (expires_at)
    WHERE expires_at IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 3. Entregas por canal
--
-- Una notificacion es UNA cosa; sus entregas son varias y fallan por separado.
-- Si el correo rebota, la copia en la campana sigue siendo valida. Mezclar
-- ambas en la misma fila obliga a un solo estado para realidades distintas.
-- -----------------------------------------------------------------------------
CREATE TABLE ecosystem_core_auth.notification_deliveries (
    delivery_id     uuid         NOT NULL DEFAULT gen_random_uuid(),
    notification_id uuid         NOT NULL,

    channel         varchar(16)  NOT NULL,   -- in_app | email | push | mqtt

    -- La direccion concreta de este canal: el topic MQTT, el correo, el token
    -- del dispositivo. Se guarda el valor usado en el momento del envio, no una
    -- referencia: si el usuario cambia de correo despues, el historial tiene que
    -- seguir diciendo a donde se mando de verdad.
    terminal        text         NOT NULL,

    status          varchar(16)  NOT NULL DEFAULT 'pending',
    attempts        smallint     NOT NULL DEFAULT 0,
    last_error      text         NULL,

    scheduled_at    timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at         timestamptz  NULL,
    created_at      timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT notification_deliveries_pkey PRIMARY KEY (delivery_id),
    CONSTRAINT notification_deliveries_channel_check CHECK (channel IN ('in_app', 'email', 'push', 'mqtt')),
    CONSTRAINT notification_deliveries_status_check  CHECK (status IN ('pending', 'sent', 'failed', 'skipped')),
    -- Un intento por canal y notificacion. Los reintentos suben `attempts`, no
    -- crean filas nuevas: asi el historial no crece sin control cuando un
    -- proveedor esta caido.
    CONSTRAINT notification_deliveries_uniq UNIQUE (notification_id, channel)
);

COMMENT ON TABLE  ecosystem_core_auth.notification_deliveries IS 'Estado de entrega de cada notificacion, separado por canal.';
COMMENT ON COLUMN ecosystem_core_auth.notification_deliveries.terminal IS 'Direccion usada en el envio: topic MQTT, correo o token de dispositivo. Se congela en el momento del envio.';
COMMENT ON COLUMN ecosystem_core_auth.notification_deliveries.status IS 'skipped = no se envio porque el usuario tiene el canal apagado y la categoria no es obligatoria.';

ALTER TABLE ecosystem_core_auth.notification_deliveries
    ADD CONSTRAINT notification_deliveries_notification_fkey
    FOREIGN KEY (notification_id) REFERENCES ecosystem_core_auth.notifications(notification_id) ON DELETE CASCADE;

-- Cola de reintentos: lo pendiente o fallido, en orden de programacion.
CREATE INDEX notification_deliveries_retry_idx
    ON ecosystem_core_auth.notification_deliveries (status, scheduled_at)
    WHERE status IN ('pending', 'failed');


-- -----------------------------------------------------------------------------
-- 4. Semilla de categorias
--
-- `auth_security` es la unica obligatoria: es la que avisa de accesos y cambios
-- de credenciales, y no puede depender de que el usuario tenga los avisos
-- encendidos.
-- -----------------------------------------------------------------------------
INSERT INTO ecosystem_core_auth.notification_categories
    (category_code, display_name_es, display_name_en, description, allows_in_app, allows_email, allows_push, is_mandatory)
VALUES
    ('auth_security',  'Seguridad',      'Security',      'Accesos, codigos de verificacion y cambios de credenciales.', true,  true,  true,  true),
    ('comment',        'Comentarios',    'Comments',      'Alguien comento tu contenido o respondio a tu comentario.',   true,  false, true,  false),
    ('like',           'Me gusta',       'Likes',         'Reacciones a tus videos y clips.',                            true,  false, false, false),
    ('follow',         'Seguidores',     'Followers',     'Nuevos seguidores de tu canal.',                              true,  false, true,  false),
    ('video',          'Publicaciones',  'Uploads',       'Un canal que sigues publico contenido nuevo.',                true,  false, true,  false),
    ('live',           'En vivo',        'Live',          'Un canal que sigues empezo una transmision.',                 true,  false, true,  false),
    ('system',         'Plataforma',     'Platform',      'Avisos del servicio y cambios en tu cuenta.',                 true,  true,  false, false)
ON CONFLICT (category_code) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 5. Permisos — mismo dueño que el resto del esquema
-- -----------------------------------------------------------------------------
ALTER TABLE ecosystem_core_auth.notification_categories   OWNER TO ecosystem_user_auth;
ALTER TABLE ecosystem_core_auth.notifications             OWNER TO ecosystem_user_auth;
ALTER TABLE ecosystem_core_auth.notification_deliveries   OWNER TO ecosystem_user_auth;

GRANT ALL ON TABLE ecosystem_core_auth.notification_categories TO ecosystem_user_auth;
GRANT ALL ON TABLE ecosystem_core_auth.notifications           TO ecosystem_user_auth;
GRANT ALL ON TABLE ecosystem_core_auth.notification_deliveries TO ecosystem_user_auth;
