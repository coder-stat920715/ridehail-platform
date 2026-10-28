package com.ridehail.location.command;

import com.ridehail.location.domain.DriverStatus;
import com.ridehail.location.domain.VehicleType;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

public record LocationUpdateRequest(
        @DecimalMin("-90") @DecimalMax("90") double lat,
        @DecimalMin("-180") @DecimalMax("180") double lng,
        @NotNull DriverStatus status,
        @NotNull VehicleType vehicleType) { }
