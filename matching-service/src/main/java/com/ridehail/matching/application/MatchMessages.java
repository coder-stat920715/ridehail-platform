package com.ridehail.matching.application;

import com.ridehail.matching.domain.VehicleType;

import java.util.UUID;

/** Wire contracts shared with trip-service's saga orchestrator (see infra/kafka-topics.md). */
public final class MatchMessages {
    private MatchMessages() { }

    public record GeoPoint(double lat, double lng) { }

    // ---- commands (consumed from outbox.event.matching-command)
    public record MatchDriver(UUID tripId, GeoPoint pickup, VehicleType vehicleType, double radiusKm) { }
    public record CancelMatch(UUID tripId) { }

    // ---- replies (published to outbox.event.matching through our outbox)
    public record DriverMatched(UUID tripId, String driverId, int etaSeconds) { }
    public record DriverMatchFailed(UUID tripId, String reason) { }
}
