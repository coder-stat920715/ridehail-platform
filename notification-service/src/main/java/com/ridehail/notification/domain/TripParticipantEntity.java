package com.ridehail.notification.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * Local READ MODEL: who is the rider / driver of a trip. Built from trip events, because most events carry only the
 * tripId. This is why notification-service needs no synchronous call to trip-service (loose coupling).
 */
@Entity
@Table(name = "trip_participant")
public class TripParticipantEntity implements Persistable<UUID> {

    @Id
    private UUID tripId;
    private String riderId;
    private String driverId;
    private Instant updatedAt;

    @Transient
    private boolean isNewEntity = true;

    protected TripParticipantEntity() { }

    public static TripParticipantEntity forRider(UUID tripId, String riderId) {
        var p = new TripParticipantEntity();
        p.tripId = tripId;
        p.riderId = riderId;
        p.updatedAt = Instant.now();
        return p;
    }

    public void assignDriver(String driverId) {
        this.driverId = driverId;
        this.updatedAt = Instant.now();
    }

    @Override public UUID getId() { return tripId; }
    @Override public boolean isNew() { return isNewEntity; }

    @PostLoad
    @PostPersist
    void markNotNew() { this.isNewEntity = false; }

    public UUID getTripId() { return tripId; }
    public String getRiderId() { return riderId; }
    public String getDriverId() { return driverId; }
}
