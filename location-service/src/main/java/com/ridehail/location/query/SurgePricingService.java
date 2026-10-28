package com.ridehail.location.query;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * Demand/supply surge. Demand = fare quotes requested in a ~2km grid cell over the last 5 minutes
 * (quote traffic is a leading indicator of demand); supply = AVAILABLE drivers within 3km.
 *   multiplier = clamp(1 + 0.25 * max(0, demand/supply - 1), 1.0, 3.0)
 * Reference implementation: production would use H3 cells, a sliding window and ML-based forecasts,
 * and would live in its own pricing-service.
 */
@Service
public class SurgePricingService {

    private static final double CELL_DEG = 0.02;
    private static final Duration WINDOW = Duration.ofMinutes(5);

    private final StringRedisTemplate redis;
    private final DriverGeoIndex index;

    public SurgePricingService(StringRedisTemplate redis, DriverGeoIndex index) {
        this.redis = redis;
        this.index = index;
    }

    public SurgeQuote quote(double lat, double lng) {
        String cell = "demand:" + Math.round(lat / CELL_DEG) + ":" + Math.round(lng / CELL_DEG);
        Long demand = redis.opsForValue().increment(cell);
        if (demand != null && demand == 1L) redis.expire(cell, WINDOW);

        int supply = index.countNearby(lat, lng, 3.0);
        int d = demand == null ? 0 : demand.intValue();
        double ratio = (double) d / Math.max(supply, 1);
        double m = Math.min(3.0, Math.max(1.0, 1.0 + 0.25 * Math.max(0.0, ratio - 1.0)));
        return new SurgeQuote(BigDecimal.valueOf(m).setScale(2, RoundingMode.HALF_UP), d, supply);
    }
}
