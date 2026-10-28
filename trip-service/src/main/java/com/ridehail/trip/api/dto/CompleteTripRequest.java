package com.ridehail.trip.api.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.Positive;

public record CompleteTripRequest(@Positive @DecimalMax("500") double distanceKm) { }
