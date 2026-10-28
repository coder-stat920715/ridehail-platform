package com.ridehail.location.query;

public record DriverContext(String driverId, String status, double lat, double lng, String vehicleType, long ts) { }
