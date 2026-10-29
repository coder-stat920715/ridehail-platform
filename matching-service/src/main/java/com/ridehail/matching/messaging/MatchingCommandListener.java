package com.ridehail.matching.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ridehail.matching.application.CandidateFinder;
import com.ridehail.matching.application.MatchMessages.CancelMatch;
import com.ridehail.matching.application.MatchMessages.MatchDriver;
import com.ridehail.matching.application.MatchingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Saga commands from trip-service. Order inside MATCH_DRIVER matters:
 *   1) skip if already processed, 2) remote search (NO transaction open), 3) short DB transaction to reserve + reply.
 */
@Component
public class MatchingCommandListener {

    private static final Logger log = LoggerFactory.getLogger(MatchingCommandListener.class);

    private final ObjectMapper mapper;
    private final CandidateFinder finder;
    private final MatchingService matching;

    public MatchingCommandListener(ObjectMapper mapper, CandidateFinder finder, MatchingService matching) {
        this.mapper = mapper;
        this.finder = finder;
        this.matching = matching;
    }

    @KafkaListener(id = "matching-commands", groupId = "matching-service", concurrency = "3",
            topics = "${ridehail.topics.matching-command}")
    public void onCommand(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String type = KafkaHeaders.get(record, "eventType");
        String id = KafkaHeaders.get(record, "id");
        if (type == null || id == null) {
            throw new IllegalArgumentException("Missing eventType/id header (not a Debezium outbox record)");
        }
        UUID eventId = UUID.fromString(id);
        log.info("Matching command {} key={}", type, record.key());

        switch (type) {
            case "MATCH_DRIVER" -> {
                MatchDriver cmd = mapper.readValue(record.value(), MatchDriver.class);
                if (!matching.isNew(eventId)) return;
                matching.assign(eventId, cmd, finder.find(cmd));
            }
            case "CANCEL_MATCH" -> matching.cancel(eventId, mapper.readValue(record.value(), CancelMatch.class).tripId());
            default -> log.debug("Ignoring command type {}", type);
        }
    }
}
