package com.ridehail.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEntity, UUID> {

    @Modifying
    @Query("delete from OutboxEntity o where o.createdAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") Instant cutoff);
}
