package com.ridehail.matching.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ridehail.matching.application.MatchingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;

/**
 * Listens to the trip lifecycle (outbox.event.trip) to learn when an offer was ACCEPTED and when a reservation must
 * be released (trip completed or cancelled). Tolerant reader: only tripId is read from the payload.
 */
@Component
public class TripEventListener {

    private static final Set<String> RELEVANT = Set.of("DRIVER_ASSIGNED", "TRIP_COMPLETED", "TRIP_CANCELLED");

    private final ObjectMapper mapper;
    private final MatchingService matching;

    public TripEventListener(ObjectMapper mapper, MatchingService matching) {
        this.mapper = mapper;
        this.matching = matching;
    }

    @KafkaListener(id = "matching-trip-events", groupId = "matching-service-trip", concurrency = "3",
            topics = "${ridehail.topics.trip-events}")
    public void onTripEvent(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String type = KafkaHeaders.get(record, "eventType");
        String id = KafkaHeaders.get(record, "id");
        if (type == null || id == null || !RELEVANT.contains(type)) return;
        UUID tripId = UUID.fromString(mapper.readTree(record.value()).path("tripId").asText());
        matching.onTripEvent(UUID.fromString(id), type, tripId);
    }
}
