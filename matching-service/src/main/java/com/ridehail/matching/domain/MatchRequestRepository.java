package com.ridehail.matching.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface MatchRequestRepository extends JpaRepository<MatchRequestEntity, UUID> {

    /** Drivers that are already reserved by an active offer/assignment. */
    @Query("select m.driverId from MatchRequestEntity m where m.state in :states and m.driverId is not null")
    List<String> findReservedDrivers(@Param("states") Collection<MatchState> states);

    /**
     * Offers older than the cutoff. FOR UPDATE SKIP LOCKED => several matching-service pods can run the expiry job
     * concurrently without processing the same row twice.
     */
    @Query(nativeQuery = true, value = """
            SELECT * FROM match_request
            WHERE state = 'OFFERED' AND offered_at < :cutoff
            ORDER BY offered_at
            LIMIT 50
            FOR UPDATE SKIP LOCKED
            """)
    List<MatchRequestEntity> lockExpiredOffers(@Param("cutoff") Instant cutoff);
}
