package com.ridehail.trip.application;

import com.ridehail.trip.client.SurgeQuote;
import com.ridehail.trip.client.SurgeQuoteClient;
import com.ridehail.trip.domain.GeoMath;
import com.ridehail.trip.domain.GeoPoint;
import com.ridehail.trip.domain.VehicleType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class FareEstimator {

    private static final double ROAD_FACTOR = 1.3;   // straight-line -> approx. road distance

    public record FareQuote(BigDecimal fare, BigDecimal surge) { }

    private final SurgeQuoteClient surgeClient;

    public FareEstimator(SurgeQuoteClient surgeClient) {
        this.surgeClient = surgeClient;
    }

    /** Remote call (deadline + breaker + neutral fallback). Must run OUTSIDE any DB transaction. */
    public FareQuote estimate(GeoPoint pickup, GeoPoint drop, VehicleType type) {
        SurgeQuote surge = surgeClient.quoteWithDeadline(pickup).join();
        double km = GeoMath.haversineKm(pickup, drop) * ROAD_FACTOR;
        return new FareQuote(price(type, km, surge.multiplier()), surge.multiplier());
    }

    public BigDecimal finalFare(VehicleType type, double distanceKm, BigDecimal surge) {
        return price(type, distanceKm, surge);
    }

    private static BigDecimal price(VehicleType type, double km, BigDecimal surge) {
        return type.baseFare()
                .add(type.perKm().multiply(BigDecimal.valueOf(km)))
                .multiply(surge)
                .setScale(2, RoundingMode.HALF_UP);
    }
}
