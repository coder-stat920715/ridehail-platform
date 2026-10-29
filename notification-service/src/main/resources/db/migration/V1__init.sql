CREATE TABLE notifications (
    id         UUID PRIMARY KEY,
    user_id    VARCHAR(64)  NOT NULL,
    event_id   UUID,
    trip_id    UUID,
    type       VARCHAR(48)  NOT NULL,
    title      VARCHAR(120) NOT NULL,
    body       VARCHAR(500) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL,
    read_at    TIMESTAMPTZ
);
CREATE INDEX ix_notifications_user_created ON notifications (user_id, created_at DESC);
CREATE INDEX ix_notifications_user_unread  ON notifications (user_id) WHERE read_at IS NULL;

-- read model: who rides / drives which trip (built from trip events)
CREATE TABLE trip_participant (
    trip_id    UUID PRIMARY KEY,
    rider_id   VARCHAR(64),
    driver_id  VARCHAR(64),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE processed_event (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
