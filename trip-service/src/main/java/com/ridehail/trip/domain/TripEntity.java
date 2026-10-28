package com.ridehail.trip.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Command-side (normalised) aggregate for a trip. Optimistic locking via @Version. */
@Entity
@Table(name = "trips")
public class TripEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String riderId;

    private String driverId;

    private double pickupLat;
    private double pickupLng;
    private double dropLat;
    private double dropLng;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private VehicleType vehicleType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TripStatus status;

    @Column(nullable = false)
    private BigDecimal surgeMultiplier;

    @Column(nullable = false)
    private BigDecimal estimatedFare;

    private BigDecimal finalFare;

    /** Wrapper type on purpose: null => Spring Data treats the entity as new (plain INSERT). */
    @Version
    private Long version;

    private Instant createdAt;
    private Instant updatedAt;

    protected TripEntity() { }

    public static TripEntity request(String riderId, GeoPoint pickup, GeoPoint drop, VehicleType vehicleType,
                                     BigDecimal estimatedFare, BigDecimal surge) {
        var t = new TripEntity();
        t.id = UUID.randomUUID();
        t.riderId = riderId;
        t.pickupLat = pickup.lat();
        t.pickupLng = pickup.lng();
        t.dropLat = drop.lat();
        t.dropLng = drop.lng();
        t.vehicleType = vehicleType;
        t.estimatedFare = estimatedFare;
        t.surgeMultiplier = surge;
        t.status = TripStatus.REQUESTED;
        t.createdAt = Instant.now();
        t.updatedAt = t.createdAt;
        return t;
    }

    public void transitionTo(TripStatus next) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalTripStateException("Trip " + id + ": illegal transition " + status + " -> " + next);
        }
        status = next;
        updatedAt = Instant.now();
    }

    public void offerTo(String driverId) {
        this.driverId = driverId;
        transitionTo(TripStatus.DRIVER_OFFERED);
    }

    public void complete(BigDecimal finalFare) {
        transitionTo(TripStatus.COMPLETED);
        this.finalFare = finalFare;
    }

    public boolean isOwnedBy(String userId) { return riderId.equals(userId); }
    public boolean isAssignedTo(String userId) { return driverId != null && driverId.equals(userId); }
    public GeoPoint pickup() { return new GeoPoint(pickupLat, pickupLng); }
    public GeoPoint drop() { return new GeoPoint(dropLat, dropLng); }

    public UUID getId() { return id; }
    public String getRiderId() { return riderId; }
    public String getDriverId() { return driverId; }
    public VehicleType getVehicleType() { return vehicleType; }
    public TripStatus getStatus() { return status; }
    public BigDecimal getSurgeMultiplier() { return surgeMultiplier; }
    public BigDecimal getEstimatedFare() { return estimatedFare; }
    public BigDecimal getFinalFare() { return finalFare; }
}
