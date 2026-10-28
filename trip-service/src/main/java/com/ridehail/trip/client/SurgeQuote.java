package com.ridehail.trip.client;

import java.math.BigDecimal;

public record SurgeQuote(BigDecimal multiplier, int demand, int supply) {
    /** Degraded-mode answer: never block a booking because pricing is slow. */
    public static final SurgeQuote NEUTRAL = new SurgeQuote(BigDecimal.ONE, 0, 0);
}
