package com.ridehail.trip.saga;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

/** Durable saga state: one row per booking, keyed by tripId. */
@Entity
@Table(name = "saga_instance")
public class SagaInstanceEntity {

    @Id
    private UUID tripId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SagaState state;

    private String holdId;
    private String failureReason;

    @Version
    private Long version;

    private Instant updatedAt;

    protected SagaInstanceEntity() { }

    public static SagaInstanceEntity begin(UUID tripId) {
        var s = new SagaInstanceEntity();
        s.tripId = tripId;
        s.state = SagaState.AWAITING_PAYMENT_HOLD;
        s.updatedAt = Instant.now();
        return s;
    }

    public void moveTo(SagaState next) {
        this.state = next;
        this.updatedAt = Instant.now();
    }

    public UUID getTripId() { return tripId; }
    public SagaState getState() { return state; }
    public String getHoldId() { return holdId; }
    public void setHoldId(String holdId) { this.holdId = holdId; }
    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
}
