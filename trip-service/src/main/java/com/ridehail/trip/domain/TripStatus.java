package com.ridehail.trip.domain;

/** Trip aggregate state machine. Illegal transitions are rejected inside the aggregate itself. */
public enum TripStatus {
    REQUESTED, PAYMENT_HELD, DRIVER_OFFERED, DRIVER_ASSIGNED, IN_PROGRESS, COMPLETED, CANCELLED;

    public boolean canTransitionTo(TripStatus next) {
        return switch (this) {
            case REQUESTED -> next == PAYMENT_HELD || next == CANCELLED;
            case PAYMENT_HELD -> next == DRIVER_OFFERED || next == CANCELLED;
            case DRIVER_OFFERED -> next == DRIVER_ASSIGNED || next == CANCELLED;
            case DRIVER_ASSIGNED -> next == IN_PROGRESS || next == CANCELLED;
            case IN_PROGRESS -> next == COMPLETED;
            case COMPLETED, CANCELLED -> false;
        };
    }
}
