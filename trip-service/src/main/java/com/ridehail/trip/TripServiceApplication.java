package com.ridehail.trip;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.ridehail.trip", "com.ridehail.outbox"})
@EntityScan(basePackages = {"com.ridehail.trip", "com.ridehail.outbox"})
@EnableJpaRepositories(basePackages = {"com.ridehail.trip", "com.ridehail.outbox"})
@EnableScheduling
public class TripServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(TripServiceApplication.class, args);
    }
}
