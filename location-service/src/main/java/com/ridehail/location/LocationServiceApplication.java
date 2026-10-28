package com.ridehail.location;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * CQRS in one deployable for the reference implementation:
 *   package .command -> write model (PostgreSQL + outbox)      -> scale on write throughput
 *   package .query   -> read model  (Redis GEO, Kafka-projected) -> scale on read QPS
 * Split into two Deployments by extracting the packages into modules; no other code changes.
 */
@SpringBootApplication(scanBasePackages = {"com.ridehail.location", "com.ridehail.outbox"})
@EntityScan(basePackages = {"com.ridehail.location", "com.ridehail.outbox"})
@EnableJpaRepositories(basePackages = {"com.ridehail.location", "com.ridehail.outbox"})
@EnableScheduling
public class LocationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LocationServiceApplication.class, args);
    }
}
