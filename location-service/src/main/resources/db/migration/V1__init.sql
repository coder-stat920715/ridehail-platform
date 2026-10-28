CREATE TABLE driver_location (
    driver_id    VARCHAR(64) PRIMARY KEY,
    lat          DOUBLE PRECISION NOT NULL,
    lng          DOUBLE PRECISION NOT NULL,
    status       VARCHAR(16) NOT NULL,
    vehicle_type VARCHAR(16) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
) WITH (fillfactor = 70);   -- leave room for HOT updates on this update-heavy table

CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    aggregatetype VARCHAR(64) NOT NULL,
    aggregateid   VARCHAR(64) NOT NULL,
    type          VARCHAR(64) NOT NULL,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_outbox_created ON outbox (created_at);
