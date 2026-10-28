package com.ridehail.trip.saga;

import com.ridehail.outbox.OutboxService;
import com.ridehail.trip.domain.IllegalTripStateException;
import com.ridehail.trip.domain.TripEntity;
import com.ridehail.trip.domain.TripNotFoundException;
import com.ridehail.trip.domain.TripRepository;
import com.ridehail.trip.domain.TripStatus;
import com.ridehail.trip.events.TripEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * ORCHESTRATED SAGA for:
 *   Book Ride -> Hold Payment -> Match Driver -> (driver accepts) -> Start Trip -> Complete Trip -> Capture Payment
 *
 * Design notes
 *  - The orchestrator owns the workflow; participants only know their own command/reply contract.
 *  - Every step = ONE local DB transaction: [update trip + update saga + insert outbox command].
 *    Debezium then ships the command. No dual write anywhere.
 *  - Replies are idempotent (processed_event ledger) and state-guarded: a stale/duplicate reply is
 *    logged and ignored rather than corrupting the workflow.
 *  - Compensations:  RELEASE_PAYMENT_HOLD (remote) then CANCEL_BOOKING (local).
 *  - "Pivot": once the trip is COMPLETED the saga can only move FORWARD. A failed capture is not
 *    compensated (we can't un-ride the ride); it raises PAYMENT_CAPTURE_FAILED for retry / dunning.
 */
@Service
public class BookingSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(BookingSagaOrchestrator.class);
    private static final double MATCH_RADIUS_KM = 3.0;

    private final SagaInstanceRepository sagas;
    private final TripRepository trips;
    private final ProcessedEventRepository processed;
    private final OutboxService outbox;

    public BookingSagaOrchestrator(SagaInstanceRepository sagas, TripRepository trips,
                                   ProcessedEventRepository processed, OutboxService outbox) {
        this.sagas = sagas;
        this.trips = trips;
        this.processed = processed;
        this.outbox = outbox;
    }

    // ------------------------------------------------------------------ start

    /** Called inside the booking transaction, right after the trip row is saved. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void start(TripEntity trip) {
        sagas.save(SagaInstanceEntity.begin(trip.getId()));
        execute(new SagaCommand.HoldPayment(trip.getId(), trip.getRiderId(), trip.getEstimatedFare()), trip);
        emit(new TripEvent.TripRequested(trip.getId(), trip.getRiderId(), trip.pickup(), trip.drop(),
                trip.getVehicleType(), trip.getEstimatedFare(), trip.getSurgeMultiplier()));
    }

    // ------------------------------------------------------- replies (Kafka)

    @Transactional
    public void onReply(UUID eventId, SagaReply reply) {
        if (processed.existsById(eventId)) {
            log.debug("Duplicate event {} ignored", eventId);
            return;
        }
        processed.save(ProcessedEventEntity.of(eventId));   // same tx: rolls back with the handler

        var saga = sagas.lockById(reply.tripId()).orElse(null);
        if (saga == null) {
            log.warn("Reply {} for unknown saga {}", reply.getClass().getSimpleName(), reply.tripId());
            return;
        }
        var trip = trips.findById(reply.tripId()).orElseThrow(() -> new TripNotFoundException(reply.tripId()));

        // Sealed interface + pattern matching => compiler-enforced exhaustiveness.
        switch (reply) {
            case SagaReply.PaymentHeld r -> {
                if (!expect(saga, r, SagaState.AWAITING_PAYMENT_HOLD)) return;
                saga.setHoldId(r.holdId());
                saga.moveTo(SagaState.AWAITING_DRIVER_MATCH);
                trip.transitionTo(TripStatus.PAYMENT_HELD);
                execute(new SagaCommand.MatchDriver(trip.getId(), trip.pickup(), trip.getVehicleType(), MATCH_RADIUS_KM), trip);
            }
            case SagaReply.PaymentHoldFailed r -> {
                if (!expect(saga, r, SagaState.AWAITING_PAYMENT_HOLD)) return;
                saga.setFailureReason(r.reason());
                saga.moveTo(SagaState.CANCELLED);
                execute(new SagaCommand.CancelBooking(trip.getId(), "Payment hold failed: " + r.reason()), trip);
            }
            case SagaReply.DriverMatched r -> {
                if (!expect(saga, r, SagaState.AWAITING_DRIVER_MATCH)) return;
                saga.moveTo(SagaState.AWAITING_DRIVER_ACCEPT);
                trip.offerTo(r.driverId());
                emit(new TripEvent.DriverOffered(trip.getId(), r.driverId(), r.etaSeconds()));
            }
            case SagaReply.DriverMatchFailed r -> {
                if (!expect(saga, r, SagaState.AWAITING_DRIVER_MATCH, SagaState.AWAITING_DRIVER_ACCEPT)) return;
                compensate(saga, trip, "No driver available: " + r.reason());
            }
            case SagaReply.PaymentReleased r -> {
                if (!expect(saga, r, SagaState.COMPENSATING)) return;
                saga.moveTo(SagaState.CANCELLED);
                execute(new SagaCommand.CancelBooking(trip.getId(),
                        saga.getFailureReason() == null ? "Cancelled" : saga.getFailureReason()), trip);
            }
            case SagaReply.PaymentCaptured r -> {
                if (!expect(saga, r, SagaState.AWAITING_PAYMENT_CAPTURE)) return;
                saga.moveTo(SagaState.COMPLETED);
            }
            case SagaReply.PaymentCaptureFailed r -> {
                if (!expect(saga, r, SagaState.AWAITING_PAYMENT_CAPTURE)) return;
                saga.setFailureReason(r.reason());
                saga.moveTo(SagaState.CAPTURE_FAILED);
                emit(new TripEvent.PaymentCaptureFailed(trip.getId(), r.reason()));
            }
        }
    }

    // ------------------------------------------- actions (REST, same saga lock)

    @Transactional
    public TripEntity driverAccepted(UUID tripId, String driverId) {
        var saga = lock(tripId, SagaState.AWAITING_DRIVER_ACCEPT);
        var trip = loadTrip(tripId);
        if (!trip.isAssignedTo(driverId)) throw new AccessDeniedException("Trip was offered to another driver");
        saga.moveTo(SagaState.DRIVER_ASSIGNED);
        trip.transitionTo(TripStatus.DRIVER_ASSIGNED);
        emit(new TripEvent.DriverAssigned(tripId, driverId));
        return trip;
    }

    @Transactional
    public TripEntity tripStarted(UUID tripId, String driverId) {
        var saga = lock(tripId, SagaState.DRIVER_ASSIGNED);
        var trip = loadTrip(tripId);
        if (!trip.isAssignedTo(driverId)) throw new AccessDeniedException("Not your trip");
        saga.moveTo(SagaState.TRIP_IN_PROGRESS);
        trip.transitionTo(TripStatus.IN_PROGRESS);
        emit(new TripEvent.TripStarted(tripId, driverId));
        return trip;
    }

    @Transactional
    public TripEntity tripCompleted(UUID tripId, String driverId, Function<TripEntity, BigDecimal> fareCalculator) {
        var saga = lock(tripId, SagaState.TRIP_IN_PROGRESS);
        var trip = loadTrip(tripId);
        if (!trip.isAssignedTo(driverId)) throw new AccessDeniedException("Not your trip");
        BigDecimal fare = fareCalculator.apply(trip);
        saga.moveTo(SagaState.AWAITING_PAYMENT_CAPTURE);
        trip.complete(fare);
        emit(new TripEvent.TripCompleted(tripId, driverId, fare));
        execute(new SagaCommand.CapturePayment(tripId, fare), trip);
        return trip;
    }

    /** Rider/driver/admin cancellation. Allowed until the ride physically starts. */
    @Transactional
    public TripEntity cancelRequested(UUID tripId, String reason) {
        var saga = lock(tripId, SagaState.AWAITING_DRIVER_MATCH, SagaState.AWAITING_DRIVER_ACCEPT, SagaState.DRIVER_ASSIGNED);
        var trip = loadTrip(tripId);
        execute(new SagaCommand.CancelMatch(tripId), trip);
        compensate(saga, trip, reason == null ? "Cancelled by user" : reason);
        return trip;
    }

    // ---------------------------------------------------------------- helpers

    /** Start compensation: release the payment hold; CANCEL_BOOKING follows on PAYMENT_RELEASED. */
    private void compensate(SagaInstanceEntity saga, TripEntity trip, String reason) {
        saga.setFailureReason(reason);
        saga.moveTo(SagaState.COMPENSATING);
        execute(new SagaCommand.ReleasePaymentHold(trip.getId(), reason), trip);
    }

    /** Single dispatch point: local compensations run in-process, everything else goes via outbox. */
    private void execute(SagaCommand cmd, TripEntity trip) {
        switch (cmd) {
            case SagaCommand.CancelBooking c -> {
                trip.transitionTo(TripStatus.CANCELLED);
                emit(new TripEvent.TripCancelled(c.tripId(), c.reason()));
            }
            default -> outbox.publish(cmd.aggregateType(), cmd.tripId().toString(), cmd.commandType(), cmd);
        }
    }

    private void emit(TripEvent event) {
        outbox.publish("trip", event.tripId().toString(), event.eventType(), event);
    }

    private boolean expect(SagaInstanceEntity saga, SagaReply reply, SagaState... allowed) {
        if (List.of(allowed).contains(saga.getState())) return true;
        log.warn("Ignoring stale/out-of-order {} for trip {} (saga state {})",
                reply.getClass().getSimpleName(), reply.tripId(), saga.getState());
        return false;
    }

    private SagaInstanceEntity lock(UUID tripId, SagaState... allowed) {
        var saga = sagas.lockById(tripId).orElseThrow(() -> new TripNotFoundException(tripId));
        if (!List.of(allowed).contains(saga.getState())) {
            throw new IllegalTripStateException("Trip " + tripId + " is in state " + saga.getState() + "; action not allowed");
        }
        return saga;
    }

    private TripEntity loadTrip(UUID tripId) {
        return trips.findById(tripId).orElseThrow(() -> new TripNotFoundException(tripId));
    }
}
