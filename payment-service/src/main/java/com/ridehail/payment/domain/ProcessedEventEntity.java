package com.ridehail.payment.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/** Idempotent-consumer ledger (Kafka is at-least-once). */
@Entity
@Table(name = "processed_event")
public class ProcessedEventEntity implements Persistable<UUID> {
    @Id
    private UUID eventId;
    private Instant processedAt;

    protected ProcessedEventEntity() { }

    public static ProcessedEventEntity of(UUID id) {
        var e = new ProcessedEventEntity();
        e.eventId = id;
        e.processedAt = Instant.now();
        return e;
    }

    @Override public UUID getId() { return eventId; }
    @Override public boolean isNew() { return true; }
}
