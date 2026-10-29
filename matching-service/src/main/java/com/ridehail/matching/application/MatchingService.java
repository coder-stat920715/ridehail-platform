package com.ridehail.matching.application;

import com.ridehail.matching.application.CandidateFinder.SearchResult;
import com.ridehail.matching.application.MatchMessages.*;
import com.ridehail.matching.client.DriverCandidate;
import com.ridehail.matching.domain.MatchRequestEntity;
import com.ridehail.matching.domain.MatchRequestRepository;
import com.ridehail.matching.domain.MatchState;
import com.ridehail.matching.domain.ProcessedEventEntity;
import com.ridehail.matching.domain.ProcessedEventRepository;
import com.ridehail.outbox.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Every method = ONE local transaction: [idempotency ledger + match state + outbox reply].
 *
 * Driver reservation is enforced twice: by the in-memory check below and, as the final safety net under
 * concurrency, by the partial unique index ux_match_driver_active (one OFFERED/ACCEPTED row per driver).
 * A race that trips the index fails the transaction; Kafka redelivers the command and the search is redone.
 */
@Service
public class MatchingService {

    private static final Logger log = LoggerFactory.getLogger(MatchingService.class);
    private static final String REPLY_AGGREGATE = "matching";          // => topic outbox.event.matching
    private static final double AVG_SPEED_KMH = 25.0;                  // urban average used for the ETA estimate

    private final MatchRequestRepository requests;
    private final ProcessedEventRepository processed;
    private final OutboxService outbox;

    public MatchingService(MatchRequestRepository requests, ProcessedEventRepository processed, OutboxService outbox) {
        this.requests = requests;
        this.processed = processed;
        this.outbox = outbox;
    }

    /** Cheap pre-check so a replayed command skips the (remote) search entirely. */
    public boolean isNew(UUID eventId) {
        return !processed.existsById(eventId);
    }

    // ------------------------------------------------------------------ MATCH_DRIVER
    @Transactional
    public void assign(UUID eventId, MatchDriver cmd, SearchResult result) {
        if (!markProcessed(eventId)) return;
        if (requests.existsById(cmd.tripId())) {                       // same trip asked twice
            log.debug("Trip {} already has a match request", cmd.tripId());
            return;
        }

        Set<String> reserved = new HashSet<>(requests.findReservedDrivers(MatchState.RESERVING));
        DriverCandidate pick = result.candidates().stream()
                .filter(c -> !reserved.contains(c.driverId()))         // re-check inside the transaction
                .findFirst().orElse(null);

        if (pick == null) {
            requests.save(MatchRequestEntity.noDrivers(cmd.tripId(), cmd.vehicleType(),
                    cmd.pickup().lat(), cmd.pickup().lng(), result.radiusKm(), "NO_DRIVERS_NEARBY"));
            log.info("Trip {}: no free driver within {} km", cmd.tripId(), result.radiusKm());
            reply(cmd.tripId(), "DRIVER_MATCH_FAILED", new DriverMatchFailed(cmd.tripId(), "NO_DRIVERS_NEARBY"));
            return;
        }

        int eta = Math.max(60, (int) Math.round(pick.distanceKm() / AVG_SPEED_KMH * 3600.0));
        requests.save(MatchRequestEntity.offered(cmd.tripId(), cmd.vehicleType(), cmd.pickup().lat(),
                cmd.pickup().lng(), pick.driverId(), pick.distanceKm(), eta, result.radiusKm()));
        log.info("Trip {}: offered to driver {} ({} km, eta {} s)", cmd.tripId(), pick.driverId(), pick.distanceKm(), eta);
        reply(cmd.tripId(), "DRIVER_MATCHED", new DriverMatched(cmd.tripId(), pick.driverId(), eta));
    }

    // ------------------------------------------------------------------ CANCEL_MATCH
    @Transactional
    public void cancel(UUID eventId, UUID tripId) {
        if (!markProcessed(eventId)) return;
        requests.findById(tripId).ifPresent(m -> {
            if (m.isReserving()) {
                m.cancel();                                            // releases the driver reservation
                log.info("Trip {}: match cancelled, driver {} released", tripId, m.getDriverId());
            }
        });
    }

    // ------------------------------------------------------------------ trip lifecycle events (outbox.event.trip)
    @Transactional
    public void onTripEvent(UUID eventId, String type, UUID tripId) {
        if (!markProcessed(eventId)) return;
        requests.findById(tripId).ifPresent(m -> {
            switch (type) {
                case "DRIVER_ASSIGNED" -> { if (m.getState() == MatchState.OFFERED) m.accept(); }
                case "TRIP_COMPLETED" -> { if (m.isReserving()) m.complete(); }
                case "TRIP_CANCELLED" -> { if (m.isReserving()) m.cancel(); }
                default -> { }
            }
        });
    }

    // ------------------------------------------------------------------ offer timeout
    /**
     * Offers the driver did not accept in time: free the driver and tell the saga (DRIVER_MATCH_FAILED), which then
     * releases the payment hold and cancels the booking. (A driver accepting at the very same instant wins the race:
     * the saga ignores the now-stale failure because it already left AWAITING_DRIVER_ACCEPT.)
     */
    @Transactional
    public int expireOffers(Instant cutoff) {
        var expired = requests.lockExpiredOffers(cutoff);
        for (MatchRequestEntity m : expired) {
            m.expire();
            log.warn("Trip {}: driver {} did not accept in time -> OFFER_EXPIRED", m.getTripId(), m.getDriverId());
            reply(m.getTripId(), "DRIVER_MATCH_FAILED", new DriverMatchFailed(m.getTripId(), "OFFER_EXPIRED"));
        }
        return expired.size();
    }

    // ------------------------------------------------------------------ helpers
    private boolean markProcessed(UUID eventId) {
        if (processed.existsById(eventId)) {
            log.debug("Duplicate event {} ignored", eventId);
            return false;
        }
        processed.save(ProcessedEventEntity.of(eventId));
        return true;
    }

    private void reply(UUID tripId, String type, Object payload) {
        outbox.publish(REPLY_AGGREGATE, tripId.toString(), type, payload);
    }
}
