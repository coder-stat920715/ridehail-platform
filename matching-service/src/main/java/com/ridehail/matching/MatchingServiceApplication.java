package com.ridehail.matching;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Saga PARTICIPANT for the "Match Driver" step. No public REST API.
 *   in : outbox.event.matching-command (MATCH_DRIVER, CANCEL_MATCH) and outbox.event.trip (lifecycle events)
 *   out: DRIVER_MATCHED / DRIVER_MATCH_FAILED through ITS OWN outbox (topic outbox.event.matching)
 */
@SpringBootApplication(scanBasePackages = {"com.ridehail.matching", "com.ridehail.outbox"})
@EntityScan(basePackages = {"com.ridehail.matching", "com.ridehail.outbox"})
@EnableJpaRepositories(basePackages = {"com.ridehail.matching", "com.ridehail.outbox"})
@EnableScheduling
public class MatchingServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(MatchingServiceApplication.class, args);
    }
}
