package com.ridehail.matching.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

/** One row per trip: the matching outcome and the driver reservation. */
@Entity
@Table(name = "match_request")
public class MatchRequestEntity {

    @Id
    private UUID tripId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private MatchState state;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private VehicleType vehicleType;

    private double pickupLat;
    private double pickupLng;

    private String driverId;
    private Double distanceKm;
    private Integer etaSeconds;
    private Double searchedRadiusKm;
    private String failureReason;

    @Version
    private Long version;

    private Instant offeredAt;
    private Instant createdAt;
    private Instant updatedAt;

    protected MatchRequestEntity() { }

    public static MatchRequestEntity offered(UUID tripId, VehicleType vt, double pickupLat, double pickupLng,
                                             String driverId, double distanceKm, int etaSeconds, double radiusKm) {
        var m = base(tripId, vt, pickupLat, pickupLng);
        m.state = MatchState.OFFERED;
        m.driverId = driverId;
        m.distanceKm = distanceKm;
        m.etaSeconds = etaSeconds;
        m.searchedRadiusKm = radiusKm;
        m.offeredAt = m.createdAt;
        return m;
    }

    public static MatchRequestEntity noDrivers(UUID tripId, VehicleType vt, double pickupLat, double pickupLng,
                                               double radiusKm, String reason) {
        var m = base(tripId, vt, pickupLat, pickupLng);
        m.state = MatchState.NO_DRIVERS;
        m.searchedRadiusKm = radiusKm;
        m.failureReason = reason;
        return m;
    }

    private static MatchRequestEntity base(UUID tripId, VehicleType vt, double lat, double lng) {
        var m = new MatchRequestEntity();
        m.tripId = tripId;
        m.vehicleType = vt;
        m.pickupLat = lat;
        m.pickupLng = lng;
        m.createdAt = Instant.now();
        m.updatedAt = m.createdAt;
        return m;
    }

    public boolean isReserving() { return MatchState.RESERVING.contains(state); }

    public void accept()  { move(MatchState.ACCEPTED); }
    public void expire()  { move(MatchState.EXPIRED); this.failureReason = "OFFER_EXPIRED"; }
    public void cancel()  { move(MatchState.CANCELLED); }
    public void complete(){ move(MatchState.COMPLETED); }

    private void move(MatchState next) { this.state = next; this.updatedAt = Instant.now(); }

    public UUID getTripId() { return tripId; }
    public MatchState getState() { return state; }
    public String getDriverId() { return driverId; }
}
