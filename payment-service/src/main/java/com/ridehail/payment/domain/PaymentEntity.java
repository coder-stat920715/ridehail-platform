package com.ridehail.payment.domain;

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

/** One payment per trip (tripId is the natural key => commands are idempotent per trip). */
@Entity
@Table(name = "payments")
public class PaymentEntity {

    @Id
    private UUID tripId;
    private String riderId;
    private String riderName;

    @Column(nullable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentState state;

    private String holdReference;
    private String failureReason;

    @Version
    private Long version;

    private Instant createdAt;
    private Instant updatedAt;

    protected PaymentEntity() { }

    public static PaymentEntity create(UUID tripId, String riderId, String riderName, BigDecimal amount) {
        var p = new PaymentEntity();
        p.tripId = tripId;
        p.riderId = riderId;
        p.riderName = riderName;
        p.amount = amount;
        p.state = PaymentState.HOLD_FAILED;   // overwritten immediately by held()/declined()
        p.createdAt = Instant.now();
        p.updatedAt = p.createdAt;
        return p;
    }

    public void held(String reference) { transition(PaymentState.HELD); this.holdReference = reference; }
    public void holdFailed(String reason) { transition(PaymentState.HOLD_FAILED); this.failureReason = reason; }
    public void released() { transition(PaymentState.RELEASED); }
    public void captured() { transition(PaymentState.CAPTURED); }
    public void captureFailed(String reason) { transition(PaymentState.CAPTURE_FAILED); this.failureReason = reason; }

    private void transition(PaymentState next) { this.state = next; this.updatedAt = Instant.now(); }

    public UUID getTripId() { return tripId; }
    public String getRiderName() { return riderName; }
    public BigDecimal getAmount() { return amount; }
    public PaymentState getState() { return state; }
    public String getHoldReference() { return holdReference; }
}
