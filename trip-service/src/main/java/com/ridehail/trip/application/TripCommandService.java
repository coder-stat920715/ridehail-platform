package com.ridehail.trip.application;

import com.ridehail.trip.api.dto.BookRideRequest;
import com.ridehail.trip.api.dto.TripResponse;
import com.ridehail.trip.domain.IllegalTripStateException;
import com.ridehail.trip.domain.TripEntity;
import com.ridehail.trip.domain.TripNotFoundException;
import com.ridehail.trip.domain.TripRepository;
import com.ridehail.trip.domain.TripStatus;
import com.ridehail.trip.saga.BookingSagaOrchestrator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/**
 * Command side of the trip bounded context.
 *
 * Booking is deliberately NOT @Transactional as a whole: the surge/fare lookup is a network call
 * and must never hold a DB connection. Only the "persist trip + start saga + outbox" part is
 * wrapped in a programmatic transaction (TransactionTemplate).
 */
@Service
public class TripCommandService {

    private static final Set<TripStatus> ACTIVE = EnumSet.of(TripStatus.REQUESTED, TripStatus.PAYMENT_HELD,
            TripStatus.DRIVER_OFFERED, TripStatus.DRIVER_ASSIGNED, TripStatus.IN_PROGRESS);

    private final TripRepository trips;
    private final BookingSagaOrchestrator saga;
    private final FareEstimator fares;
    private final TransactionTemplate tx;

    public TripCommandService(TripRepository trips, BookingSagaOrchestrator saga, FareEstimator fares,
                              TransactionTemplate tx) {
        this.trips = trips;
        this.saga = saga;
        this.fares = fares;
        this.tx = tx;
    }

    public TripResponse bookRide(String riderId, String riderName, BookRideRequest req) {
        if (trips.existsByRiderIdAndStatusIn(riderId, ACTIVE)) {
            throw new IllegalTripStateException("Rider already has an active trip");
        }
        var quote = fares.estimate(req.pickup(), req.drop(), req.vehicleType());   // no tx open here

        return tx.execute(status -> {
            TripEntity trip = trips.save(TripEntity.request(riderId, riderName, req.pickup(), req.drop(),
                    req.vehicleType(), quote.fare(), quote.surge()));
            saga.start(trip);                                                       // + outbox rows, same commit
            return TripResponse.from(trip);
        });
    }

    @Transactional(readOnly = true)
    public TripResponse get(UUID tripId) {
        return trips.findById(tripId).map(TripResponse::from).orElseThrow(() -> new TripNotFoundException(tripId));
    }

    @Transactional
    public TripResponse accept(UUID tripId, String driverId) {
        return TripResponse.from(saga.driverAccepted(tripId, driverId));
    }

    @Transactional
    public TripResponse start(UUID tripId, String driverId) {
        return TripResponse.from(saga.tripStarted(tripId, driverId));
    }

    @Transactional
    public TripResponse complete(UUID tripId, String driverId, double distanceKm) {
        return TripResponse.from(saga.tripCompleted(tripId, driverId,
                trip -> fares.finalFare(trip.getVehicleType(), distanceKm, trip.getSurgeMultiplier())));
    }

    @Transactional
    public TripResponse cancel(UUID tripId, String reason) {
        return TripResponse.from(saga.cancelRequested(tripId, reason));
    }
}
