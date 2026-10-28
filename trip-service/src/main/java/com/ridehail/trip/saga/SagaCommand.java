package com.ridehail.trip.saga;

import com.ridehail.trip.domain.GeoPoint;
import com.ridehail.trip.domain.VehicleType;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Commands issued by the orchestrator. Remote ones go through the outbox to the participant's
 * command topic (aggregate type => topic outbox.event.<aggregateType>); CancelBooking is a LOCAL
 * compensating action executed by the orchestrator itself.
 */
public sealed interface SagaCommand {
    UUID tripId();
    String commandType();
    String aggregateType();

    record HoldPayment(UUID tripId, String riderId, BigDecimal amount) implements SagaCommand {
        public String commandType() { return "HOLD_PAYMENT"; }
        public String aggregateType() { return "payment-command"; }
    }
    record CapturePayment(UUID tripId, BigDecimal amount) implements SagaCommand {
        public String commandType() { return "CAPTURE_PAYMENT"; }
        public String aggregateType() { return "payment-command"; }
    }
    record ReleasePaymentHold(UUID tripId, String reason) implements SagaCommand {
        public String commandType() { return "RELEASE_PAYMENT_HOLD"; }
        public String aggregateType() { return "payment-command"; }
    }
    record MatchDriver(UUID tripId, GeoPoint pickup, VehicleType vehicleType, double radiusKm) implements SagaCommand {
        public String commandType() { return "MATCH_DRIVER"; }
        public String aggregateType() { return "matching-command"; }
    }
    record CancelMatch(UUID tripId) implements SagaCommand {
        public String commandType() { return "CANCEL_MATCH"; }
        public String aggregateType() { return "matching-command"; }
    }
    /** Local compensation: moves the trip to CANCELLED and emits TRIP_CANCELLED. */
    record CancelBooking(UUID tripId, String reason) implements SagaCommand {
        public String commandType() { return "CANCEL_BOOKING"; }
        public String aggregateType() { return "trip"; }
    }
}
