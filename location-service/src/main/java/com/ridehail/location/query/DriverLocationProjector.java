package com.ridehail.location.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ridehail.location.domain.DriverLocationUpdated;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that keeps the Redis read model in sync with the write model.
 * The record key is the driverId (set by Debezium from outbox.aggregateid), so a driver's updates
 * are processed in order within one partition. Parse failures go to the DLT via the error handler.
 */
@Component
public class DriverLocationProjector {

    private final ObjectMapper mapper;
    private final DriverGeoIndex index;

    public DriverLocationProjector(ObjectMapper mapper, DriverGeoIndex index) {
        this.mapper = mapper;
        this.index = index;
    }

    @KafkaListener(id = "location-projector", groupId = "location-query-projector", concurrency = "6",
            topics = "${ridehail.topics.driver-location}")
    public void onLocation(String payload) throws JsonProcessingException {
        index.apply(mapper.readValue(payload, DriverLocationUpdated.class));
    }
}
