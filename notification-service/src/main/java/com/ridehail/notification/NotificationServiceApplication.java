package com.ridehail.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Pure event CONSUMER (no outbox, no Debezium of its own): turns trip lifecycle events from
 * outbox.event.trip into per-user notifications (in-app inbox + simulated push/SMS) and exposes the inbox API.
 */
@SpringBootApplication
public class NotificationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
