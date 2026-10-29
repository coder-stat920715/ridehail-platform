package com.ridehail.matching.application;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class OfferExpiryJob {

    private final MatchingService matching;
    private final Duration offerTimeout;

    public OfferExpiryJob(MatchingService matching,
                          @Value("${ridehail.matching.offer-timeout-seconds:60}") long offerTimeoutSeconds) {
        this.matching = matching;
        this.offerTimeout = Duration.ofSeconds(offerTimeoutSeconds);
    }

    @Scheduled(fixedDelayString = "PT5S", initialDelayString = "PT10S")
    public void run() {
        matching.expireOffers(Instant.now().minus(offerTimeout));
    }
}
