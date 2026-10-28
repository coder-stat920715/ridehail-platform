package com.ridehail.trip.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.Optional;

/**
 * Reads a driver's availability + last known location from the location read-model (used by ABAC).
 *
 * Aspect order is Retry( CircuitBreaker( call ) ): each attempt is recorded by the breaker, and
 * once the breaker is OPEN, CallNotPermittedException is NOT retried (see retry ignore-exceptions).
 * Retries use FULL JITTER backoff (see ResilienceConfig) to avoid synchronised retry storms.
 *
 * The fallback FAILS CLOSED: if we can't prove the driver is near the pickup, the answer is
 * "no context" and the ABAC rule denies. Security decisions must not fail open.
 */
@Component
public class DriverContextClient {

    private static final Logger log = LoggerFactory.getLogger(DriverContextClient.class);
    private final RestClient client;

    public DriverContextClient(RestClient locationRestClient) {
        this.client = locationRestClient;
    }

    @Retry(name = "driverContext")
    @CircuitBreaker(name = "driverContext", fallbackMethod = "failClosed")
    public Optional<DriverContext> find(String driverId) {
        try {
            return Optional.ofNullable(client.get()
                    .uri("/internal/v1/drivers/{id}", driverId)
                    .retrieve()
                    .body(DriverContext.class));
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();               // business answer, not a failure
        }
    }

    @SuppressWarnings("unused")
    private Optional<DriverContext> failClosed(String driverId, Throwable t) {
        log.warn("Driver context unavailable for {} ({}); denying by default", driverId, t.toString());
        return Optional.empty();
    }
}
