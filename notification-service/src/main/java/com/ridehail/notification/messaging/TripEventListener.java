package com.ridehail.notification.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.ridehail.notification.application.NotificationService;
import com.ridehail.notification.channel.ChannelDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Consumes trip lifecycle events published by trip-service's outbox (via Debezium).
 * Flow per record: (1) DB transaction -> inbox rows, (2) AFTER commit -> push/SMS.
 * If step 2 fails the inbox is already saved; if step 1 fails Kafka retries (then DLT).
 */
@Component
public class TripEventListener {

    private static final Logger log = LoggerFactory.getLogger(TripEventListener.class);

    private final NotificationService service;
    private final ChannelDispatcher channels;

    public TripEventListener(NotificationService service, ChannelDispatcher channels) {
        this.service = service;
        this.channels = channels;
    }

    @KafkaListener(id = "notification-trip-events", groupId = "notification-service", concurrency = "3",
            topics = "${ridehail.topics.trip-events}")
    public void onTripEvent(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String type = header(record, "eventType");
        String id = header(record, "id");
        if (type == null || id == null) {
            throw new IllegalArgumentException("Missing eventType/id header (not a Debezium outbox record)");
        }
        var external = service.process(UUID.fromString(id), type, record.value());     // transactional
        if (!external.isEmpty()) {
            log.debug("Delivering {} external notification(s) for {}", external.size(), type);
            channels.deliver(external);                                               // after commit
        }
    }

    private static String header(ConsumerRecord<?, ?> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
