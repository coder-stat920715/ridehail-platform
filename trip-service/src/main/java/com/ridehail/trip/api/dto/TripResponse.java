package com.ridehail.trip.api.dto;

import com.ridehail.trip.domain.TripEntity;
import com.ridehail.trip.domain.TripStatus;

import java.math.BigDecimal;
import java.util.UUID;

public record TripResponse(UUID tripId, TripStatus status, String riderId, String driverId,
                           BigDecimal estimatedFare, BigDecimal finalFare, BigDecimal surgeMultiplier) {

    public static TripResponse from(TripEntity t) {
        return new TripResponse(t.getId(), t.getStatus(), t.getRiderId(), t.getDriverId(),
                t.getEstimatedFare(), t.getFinalFare(), t.getSurgeMultiplier());
    }
}
