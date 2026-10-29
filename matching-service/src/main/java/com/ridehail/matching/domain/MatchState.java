package com.ridehail.matching.domain;

import java.util.List;

public enum MatchState {
    OFFERED,      // a driver was selected and the trip is waiting for the driver to accept
    ACCEPTED,     // trip-service published DRIVER_ASSIGNED
    EXPIRED,      // driver did not accept within the offer timeout
    NO_DRIVERS,   // nobody available within the (expanded) radius
    CANCELLED,    // CANCEL_MATCH / TRIP_CANCELLED
    COMPLETED;    // trip finished

    /** States that RESERVE the driver: a driver can be in at most one of these across all trips. */
    public static final List<MatchState> RESERVING = List.of(OFFERED, ACCEPTED);
}
