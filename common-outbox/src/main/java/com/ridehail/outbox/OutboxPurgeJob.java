package com.ridehail.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

/**
 * Keeps the outbox table small. Safe to delete aged rows: Debezium reads INSERTs from the WAL
 * (replication slot retains them), not from the table, so deleting never loses an event.
 * Requires @EnableScheduling in the host application.
 */
@Component
public class OutboxPurgeJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxPurgeJob.class);
    private final OutboxRepository repository;

    public OutboxPurgeJob(OutboxRepository repository) {
        this.repository = repository;
    }

    @Scheduled(fixedDelayString = "PT5M", initialDelayString = "PT1M")
    @Transactional
    public void purge() {
        int n = repository.deleteOlderThan(Instant.now().minus(Duration.ofHours(1)));
        if (n > 0) log.debug("Purged {} outbox rows", n);
    }
}
