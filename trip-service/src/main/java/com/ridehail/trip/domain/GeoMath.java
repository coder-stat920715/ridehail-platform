package com.ridehail.trip.domain;

public final class GeoMath {
    private static final double EARTH_RADIUS_KM = 6371.0088;
    private GeoMath() { }

    public static double haversineKm(double lat1, double lng1, double lat2, double lng2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(a));
    }

    public static double haversineKm(GeoPoint a, GeoPoint b) {
        return haversineKm(a.lat(), a.lng(), b.lat(), b.lng());
    }
}
