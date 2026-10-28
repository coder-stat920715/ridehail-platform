package com.ridehail.trip.resilience;

import io.github.resilience4j.core.IntervalFunction;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * "Full jitter" exponential backoff (AWS Architecture Blog):
 *     sleep = random(0, min(cap, base * multiplier^(attempt-1)))
 * Spreads retries uniformly across the whole window, which de-correlates clients far better than
 * Resilience4j's built-in randomizedWaitFactor (which only jitters +/- a fraction around the mean).
 */
public final class FullJitterIntervalFunction implements IntervalFunction {

    private final long baseMs;
    private final double multiplier;
    private final long capMs;

    public FullJitterIntervalFunction(Duration base, double multiplier, Duration cap) {
        this.baseMs = base.toMillis();
        this.multiplier = multiplier;
        this.capMs = cap.toMillis();
    }

    @Override
    public Long apply(Integer attempt) {                       // attempt starts at 1
        double window = Math.min(capMs, baseMs * Math.pow(multiplier, attempt - 1));
        return ThreadLocalRandom.current().nextLong(1, (long) window + 1);
    }
}
