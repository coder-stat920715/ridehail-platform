package com.ridehail.matching.client;

import com.ridehail.matching.domain.VehicleType;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Arrays;
import java.util.List;

/**
 * Calls the CQRS QUERY side of location-service (Redis GEO).
 *
 * Deliberately NO fallback: if location-service is down we must NOT answer "no drivers" (that would cancel
 * every booking and release payments). The exception propagates, the Kafka error handler retries the command with
 * backoff, and only after ~30 s does it go to the DLT. Resilience4j still protects us: Retry (jitter) absorbs blips
 * and the CircuitBreaker stops us hammering a sick dependency.
 */
@Component
public class DriverSearchClient {

    private final RestClient client;

    public DriverSearchClient(RestClient locationRestClient) {
        this.client = locationRestClient;
    }

    @Retry(name = "driverSearch")
    @CircuitBreaker(name = "driverSearch")
    public List<DriverCandidate> nearby(double lat, double lng, VehicleType type, double radiusKm, int limit) {
        DriverCandidate[] body = client.get()
                .uri("/internal/v1/drivers/nearby?lat={lat}&lng={lng}&radiusKm={r}&vehicleType={t}&limit={n}",
                        lat, lng, radiusKm, type.name(), limit)
                .retrieve()
                .body(DriverCandidate[].class);
        return body == null ? List.of() : Arrays.asList(body);
    }
}
