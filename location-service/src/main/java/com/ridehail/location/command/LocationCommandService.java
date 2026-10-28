package com.ridehail.location.command;

import com.ridehail.location.domain.DriverLocationUpdated;
import com.ridehail.outbox.OutboxService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * CQRS COMMAND side: state change + outbox row in ONE transaction. Debezium relays the row to
 * Kafka; the query side projects it into Redis. The caller never touches Redis or Kafka directly.
 *
 * Scale note: at ~100k drivers pinging every 4s this is ~25k upserts/s (+ the same in outbox
 * inserts). Mitigations: client-side distance/time throttling, batching, partitioned Postgres, or
 * - since raw pings are ephemeral - publishing them straight to Kafka and reserving the outbox for
 * business state changes. The pattern is kept here because it is what the requirements specify.
 */
@Service
public class LocationCommandService {

    private final DriverLocationRepository repository;
    private final OutboxService outbox;

    public LocationCommandService(DriverLocationRepository repository, OutboxService outbox) {
        this.repository = repository;
        this.outbox = outbox;
    }

    @Transactional
    public void record(String driverId, LocationUpdateRequest req) {
        Instant now = Instant.now();
        repository.upsert(driverId, req.lat(), req.lng(), req.status().name(), req.vehicleType().name(), now);
        // aggregateId = driverId => Kafka key => all updates of a driver stay ordered in one partition.
        outbox.publish("driver-location", driverId, "DRIVER_LOCATION_UPDATED",
                new DriverLocationUpdated(driverId, req.lat(), req.lng(), req.status(), req.vehicleType(), now));
    }
}
