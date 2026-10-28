package com.ridehail.trip.saga;

public enum SagaState {
    AWAITING_PAYMENT_HOLD,
    AWAITING_DRIVER_MATCH,
    AWAITING_DRIVER_ACCEPT,
    DRIVER_ASSIGNED,
    TRIP_IN_PROGRESS,
    AWAITING_PAYMENT_CAPTURE,
    COMPLETED,
    COMPENSATING,          // RELEASE_PAYMENT_HOLD in flight
    CANCELLED,
    CAPTURE_FAILED         // forward-recovery needed (trip already happened)
}
