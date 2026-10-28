package com.ridehail.trip.api.dto;

import com.ridehail.trip.domain.GeoPoint;
import com.ridehail.trip.domain.VehicleType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record BookRideRequest(
        @NotNull @Valid GeoPoint pickup,
        @NotNull @Valid GeoPoint drop,
        @NotNull VehicleType vehicleType) { }
