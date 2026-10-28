CREATE TABLE trips (
    id               UUID PRIMARY KEY,
    rider_id         VARCHAR(64)      NOT NULL,
    driver_id        VARCHAR(64),
    pickup_lat       DOUBLE PRECISION NOT NULL,
    pickup_lng       DOUBLE PRECISION NOT NULL,
    drop_lat         DOUBLE PRECISION NOT NULL,
    drop_lng         DOUBLE PRECISION NOT NULL,
    vehicle_type     VARCHAR(16)      NOT NULL,
    status           VARCHAR(24)      NOT NULL,
    surge_multiplier NUMERIC(4,2)     NOT NULL,
    estimated_fare   NUMERIC(10,2)    NOT NULL,
    final_fare       NUMERIC(10,2),
    version          BIGINT           NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ      NOT NULL,
    updated_at       TIMESTAMPTZ      NOT NULL
);
CREATE INDEX ix_trips_rider  ON trips (rider_id);
CREATE INDEX ix_trips_driver ON trips (driver_id);
-- Invariant enforced by the DB, not by hope: one active trip per rider.
CREATE UNIQUE INDEX ux_trips_one_active_per_rider ON trips (rider_id)
    WHERE status IN ('REQUESTED','PAYMENT_HELD','DRIVER_OFFERED','DRIVER_ASSIGNED','IN_PROGRESS');

CREATE TABLE saga_instance (
    trip_id        UUID PRIMARY KEY,
    state          VARCHAR(32) NOT NULL,
    hold_id        VARCHAR(64),
    failure_reason VARCHAR(255),
    version        BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE processed_event (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);

-- Column names match Debezium EventRouter defaults.
CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    aggregatetype VARCHAR(64) NOT NULL,
    aggregateid   VARCHAR(64) NOT NULL,
    type          VARCHAR(64) NOT NULL,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_outbox_created ON outbox (created_at);
