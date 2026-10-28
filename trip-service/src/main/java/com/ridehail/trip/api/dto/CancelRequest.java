package com.ridehail.trip.api.dto;

import jakarta.validation.constraints.Size;

public record CancelRequest(@Size(max = 200) String reason) { }
