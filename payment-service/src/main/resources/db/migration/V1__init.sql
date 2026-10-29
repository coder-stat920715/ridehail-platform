CREATE TABLE payments (
    trip_id        UUID PRIMARY KEY,
    rider_id       VARCHAR(64),
    rider_name     VARCHAR(128),
    amount         NUMERIC(10,2) NOT NULL,
    state          VARCHAR(24)   NOT NULL,
    hold_reference VARCHAR(64),
    failure_reason VARCHAR(255),
    version        BIGINT        NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ   NOT NULL,
    updated_at     TIMESTAMPTZ   NOT NULL
);

CREATE TABLE processed_event (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    aggregatetype VARCHAR(64) NOT NULL,
    aggregateid   VARCHAR(64) NOT NULL,
    type          VARCHAR(64) NOT NULL,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_outbox_created ON outbox (created_at);
