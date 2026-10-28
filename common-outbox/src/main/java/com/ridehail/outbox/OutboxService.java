package com.ridehail.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Writes integration events/commands to the outbox table.
 *
 * ARCHITECTURAL DECISION: Propagation.MANDATORY. The whole point of the pattern is that the outbox
 * row commits atomically with the business change. If someone calls this outside a transaction we
 * fail fast instead of silently re-introducing the dual-write problem.
 *
 * There is NO Kafka producer here. Debezium tails the PostgreSQL WAL and publishes the row.
 */
@Service
public class OutboxService {

    private final OutboxRepository repository;
    private final ObjectMapper mapper;

    public OutboxService(OutboxRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID publish(String aggregateType, String aggregateId, String eventType, Object payload) {
        final String json;
        try {
            json = mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise outbox payload for " + eventType, e);
        }
        var row = new OutboxEntity(UUID.randomUUID(), aggregateType, aggregateId, eventType, json);
        repository.save(row);
        return row.getId();
    }
}
