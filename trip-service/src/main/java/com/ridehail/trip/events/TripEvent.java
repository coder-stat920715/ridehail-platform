package com.ridehail.trip.events;

import com.ridehail.trip.domain.GeoPoint;
import com.ridehail.trip.domain.VehicleType;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Integration events published on outbox aggregate type "trip" (=> topic outbox.event.trip).
 * Consumed by notification-service, analytics, the pricing/demand projector, etc.
 */
public sealed interface TripEvent {
    UUID tripId();
    String eventType();

    record TripRequested(UUID tripId, String riderId, GeoPoint pickup, GeoPoint drop, VehicleType vehicleType,
                         BigDecimal estimatedFare, BigDecimal surgeMultiplier) implements TripEvent {
        public String eventType() { return "TRIP_REQUESTED"; }
    }
    record DriverOffered(UUID tripId, String driverId, int etaSeconds) implements TripEvent {
        public String eventType() { return "DRIVER_OFFERED"; }
    }
    record DriverAssigned(UUID tripId, String driverId) implements TripEvent {
        public String eventType() { return "DRIVER_ASSIGNED"; }
    }
    record TripStarted(UUID tripId, String driverId) implements TripEvent {
        public String eventType() { return "TRIP_STARTED"; }
    }
    record TripCompleted(UUID tripId, String driverId, BigDecimal finalFare) implements TripEvent {
        public String eventType() { return "TRIP_COMPLETED"; }
    }
    record TripCancelled(UUID tripId, String reason) implements TripEvent {
        public String eventType() { return "TRIP_CANCELLED"; }
    }
    record PaymentCaptureFailed(UUID tripId, String reason) implements TripEvent {
        public String eventType() { return "PAYMENT_CAPTURE_FAILED"; }
    }
}
