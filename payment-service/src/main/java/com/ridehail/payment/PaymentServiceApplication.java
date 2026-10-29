package com.ridehail.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Saga PARTICIPANT. No public REST API: it only reacts to commands from trip-service's orchestrator
 * (topic outbox.event.payment-command) and answers through ITS OWN outbox (topic outbox.event.payment),
 * relayed by Debezium. The card/PSP is simulated (see SimulatedPaymentGateway).
 */
@SpringBootApplication(scanBasePackages = {"com.ridehail.payment", "com.ridehail.outbox"})
@EntityScan(basePackages = {"com.ridehail.payment", "com.ridehail.outbox"})
@EnableJpaRepositories(basePackages = {"com.ridehail.payment", "com.ridehail.outbox"})
@EnableScheduling
public class PaymentServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
