package com.ridehail.trip.saga;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/** Idempotent-consumer ledger: Kafka is at-least-once, so we record every handled event id. */
@Entity
@Table(name = "processed_event")
public class ProcessedEventEntity implements Persistable<UUID> {

    @Id
    private UUID eventId;
    private Instant processedAt;

    protected ProcessedEventEntity() { }

    public static ProcessedEventEntity of(UUID eventId) {
        var e = new ProcessedEventEntity();
        e.eventId = eventId;
        e.processedAt = Instant.now();
        return e;
    }

    @Override public UUID getId() { return eventId; }
    @Override public boolean isNew() { return true; }
}
