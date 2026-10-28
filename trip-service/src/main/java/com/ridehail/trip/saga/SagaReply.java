package com.ridehail.trip.saga;

import java.util.UUID;

/** Replies from saga participants (payment-service, matching-service). */
public sealed interface SagaReply {
    UUID tripId();

    record PaymentHeld(UUID tripId, String holdId) implements SagaReply { }
    record PaymentHoldFailed(UUID tripId, String reason) implements SagaReply { }
    record DriverMatched(UUID tripId, String driverId, int etaSeconds) implements SagaReply { }
    record DriverMatchFailed(UUID tripId, String reason) implements SagaReply { }
    record PaymentReleased(UUID tripId) implements SagaReply { }
    record PaymentCaptured(UUID tripId) implements SagaReply { }
    record PaymentCaptureFailed(UUID tripId, String reason) implements SagaReply { }
}
