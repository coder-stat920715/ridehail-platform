package com.ridehail.location.query;

public record NearbyDriver(String driverId, double distanceKm, double lat, double lng) { }
