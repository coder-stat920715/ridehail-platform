package com.ridehail.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.ColumnTransformer;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * Transactional-outbox row. Column names (aggregatetype, aggregateid, type, payload) are exactly
 * what Debezium's EventRouter SMT expects by default, so no custom field mapping is needed.
 *
 * Implements Persistable (always "new") so Spring Data issues a plain INSERT instead of the
 * SELECT-then-INSERT that merge() would do for an entity with an assigned id.
 */
@Entity
@Table(name = "outbox")
public class OutboxEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    /** Drives Debezium topic routing: topic = "outbox.event." + aggregatetype. */
    @Column(name = "aggregatetype", nullable = false, updatable = false)
    private String aggregateType;

    /** Becomes the Kafka record key => per-aggregate ordering within a partition. */
    @Column(name = "aggregateid", nullable = false, updatable = false)
    private String aggregateId;

    /** Event / command type; exposed to consumers as the "eventType" Kafka header. */
    @Column(name = "type", nullable = false, updatable = false)
    private String type;

    /** jsonb column; the cast lets Hibernate bind a plain String parameter. */
    @ColumnTransformer(write = "?::jsonb")
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false, updatable = false)
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected OutboxEntity() { }

    public OutboxEntity(UUID id, String aggregateType, String aggregateId, String type, String payload) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.type = type;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    @Override public UUID getId() { return id; }
    @Override public boolean isNew() { return true; }
    public String getAggregateType() { return aggregateType; }
    public String getAggregateId() { return aggregateId; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }
}
