package com.ridehail.trip.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Consumes participant replies that Debezium's EventRouter published from THEIR outbox tables.
 * The router puts the outbox row id in header "id" and the row type in header "eventType",
 * and the (expanded) JSON payload as the record value.
 *
 * Offsets are committed only after the handler's DB transaction commits (AckMode.RECORD), so
 * delivery is at-least-once; the processed_event ledger makes handling effectively exactly-once.
 */
@Component
public class SagaReplyListener {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyListener.class);

    private final SagaReplyParser parser;
    private final BookingSagaOrchestrator orchestrator;

    public SagaReplyListener(SagaReplyParser parser, BookingSagaOrchestrator orchestrator) {
        this.parser = parser;
        this.orchestrator = orchestrator;
    }

    @KafkaListener(id = "trip-saga-replies", groupId = "trip-saga", concurrency = "3",
            topics = {"${ridehail.topics.payment-events}", "${ridehail.topics.matching-events}"})
    public void onMessage(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String eventType = header(record, "eventType");
        String eventId = header(record, "id");
        if (eventType == null || eventId == null) {
            log.warn("Skipping record without eventType/id headers at {}-{}@{}",
                    record.topic(), record.partition(), record.offset());
            return;
        }
        var reply = parser.parse(eventType, record.value());
        if (reply.isEmpty()) {
            log.debug("Ignoring non-saga event {}", eventType);
            return;
        }
        orchestrator.onReply(UUID.fromString(eventId), reply.get());
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
