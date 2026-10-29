package com.ridehail.matching.client;

/** Shape returned by location-service GET /internal/v1/drivers/nearby (nearest first). */
public record DriverCandidate(String driverId, double distanceKm, double lat, double lng) { }
