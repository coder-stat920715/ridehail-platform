package com.ridehail.matching.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

final class KafkaHeaders {
    private KafkaHeaders() { }

    /** Debezium's EventRouter adds headers "id" (outbox row id) and "eventType". */
    static String get(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
