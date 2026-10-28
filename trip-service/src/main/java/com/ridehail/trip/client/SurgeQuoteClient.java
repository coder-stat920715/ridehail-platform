package com.ridehail.trip.client;

import com.ridehail.trip.domain.GeoPoint;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Surge lookup with the full "downstream REST call" protection stack:
 *   TimeLimiter (hard deadline) -> Bulkhead (cap concurrency, semaphore: ideal for virtual threads)
 *   -> CircuitBreaker (CLOSED/OPEN/HALF_OPEN) -> graceful fallback (multiplier 1.0).
 *
 * @TimeLimiter only works on CompletionStage/Future returns, hence the async signature. The call
 * runs on a virtual thread (Executors.newVirtualThreadPerTaskExecutor) so waiting is cheap.
 * Only the OUTERMOST annotation (CircuitBreaker) declares the fallback, so that timeouts and
 * bulkhead rejections are also converted into the neutral quote - and still counted by the breaker.
 */
@Component
public class SurgeQuoteClient {

    private static final Logger log = LoggerFactory.getLogger(SurgeQuoteClient.class);

    private final RestClient client;
    private final ExecutorService virtualThreads;

    public SurgeQuoteClient(RestClient locationRestClient, ExecutorService virtualThreadExecutor) {
        this.client = locationRestClient;
        this.virtualThreads = virtualThreadExecutor;
    }

    @CircuitBreaker(name = "surgeQuote", fallbackMethod = "neutral")
    @TimeLimiter(name = "surgeQuote")
    @Bulkhead(name = "surgeQuote", type = Bulkhead.Type.SEMAPHORE)
    public CompletableFuture<SurgeQuote> quoteWithDeadline(GeoPoint p) {
        return CompletableFuture.supplyAsync(() -> client.get()
                .uri("/internal/v1/surge?lat={lat}&lng={lng}", p.lat(), p.lng())
                .retrieve()
                .body(SurgeQuote.class), virtualThreads);
    }

    @SuppressWarnings("unused")
    private CompletableFuture<SurgeQuote> neutral(GeoPoint p, Throwable t) {
        log.warn("Surge lookup degraded to 1.0x ({}: {})", t.getClass().getSimpleName(), t.getMessage());
        return CompletableFuture.completedFuture(SurgeQuote.NEUTRAL);
    }
}
