package com.ridehail.location.domain;

import java.time.Instant;

/** Integration event on aggregate type "driver-location" (=> topic outbox.event.driver-location). */
public record DriverLocationUpdated(String driverId, double lat, double lng, DriverStatus status,
                                    VehicleType vehicleType, Instant ts) { }
