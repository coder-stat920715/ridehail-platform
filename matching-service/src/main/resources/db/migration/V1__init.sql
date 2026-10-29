CREATE TABLE match_request (
    trip_id            UUID PRIMARY KEY,
    state              VARCHAR(16)      NOT NULL,
    vehicle_type       VARCHAR(16)      NOT NULL,
    pickup_lat         DOUBLE PRECISION NOT NULL,
    pickup_lng         DOUBLE PRECISION NOT NULL,
    driver_id          VARCHAR(64),
    distance_km        DOUBLE PRECISION,
    eta_seconds        INT,
    searched_radius_km DOUBLE PRECISION,
    failure_reason     VARCHAR(128),
    version            BIGINT           NOT NULL DEFAULT 0,
    offered_at         TIMESTAMPTZ,
    created_at         TIMESTAMPTZ      NOT NULL,
    updated_at         TIMESTAMPTZ      NOT NULL
);

-- A driver can hold at most ONE active offer/assignment across all trips (reservation invariant).
CREATE UNIQUE INDEX ux_match_driver_active ON match_request (driver_id) WHERE state IN ('OFFERED','ACCEPTED');
CREATE INDEX ix_match_offered ON match_request (offered_at) WHERE state = 'OFFERED';

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
