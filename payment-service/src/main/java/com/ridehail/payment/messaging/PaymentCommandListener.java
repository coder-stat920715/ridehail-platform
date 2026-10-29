package com.ridehail.payment.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.ridehail.payment.application.PaymentCommandHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * NON-BLOCKING retries with @RetryableTopic. A failing command is re-published to
 *   outbox.event.payment-command-retry-1000, -retry-2000, -retry-4000 (topic name carries the delay)
 * and finally to outbox.event.payment-command.DLT, so one poison/failing message never blocks the partition.
 * Malformed payloads (JSON/IllegalArgument) skip the retries and go straight to the DLT.
 * Spring Boot auto-creates the retry/DLT topics (KafkaAdmin) - pre-create them in production.
 */
@Component
public class PaymentCommandListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandListener.class);
    private final PaymentCommandHandler handler;

    public PaymentCommandListener(PaymentCommandHandler handler) {
        this.handler = handler;
    }

    @RetryableTopic(attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 8000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = ".DLT",
            exclude = {JsonProcessingException.class, IllegalArgumentException.class},
            numPartitions = "3", replicationFactor = "1")
    @KafkaListener(id = "payment-commands", groupId = "payment-service", concurrency = "3",
            topics = "${ridehail.topics.payment-command}")
    public void onCommand(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String type = header(record, "eventType");
        String id = header(record, "id");
        if (type == null || id == null) {
            throw new IllegalArgumentException("Missing eventType/id header (not a Debezium outbox record)");
        }
        log.info("Payment command {} for key {}", type, record.key());
        handler.handle(UUID.fromString(id), type, record.value());
    }

    /** Terminal handler: alert + park for manual replay. */
    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        log.error("DEAD LETTER on {} key={} headers={} value={}", record.topic(), record.key(),
                record.headers(), record.value());
    }

    private static String header(ConsumerRecord<?, ?> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
