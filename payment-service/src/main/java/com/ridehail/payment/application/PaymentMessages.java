package com.ridehail.payment.application;

import java.math.BigDecimal;
import java.util.UUID;

/** Wire contracts. Commands come from trip-service's saga orchestrator; replies go back through our outbox. */
public final class PaymentMessages {
    private PaymentMessages() { }

    // ---- commands (consumed)
    public record HoldPayment(UUID tripId, String riderId, String riderName, BigDecimal amount) { }
    public record CapturePayment(UUID tripId, BigDecimal amount) { }
    public record ReleasePaymentHold(UUID tripId, String reason) { }

    // ---- replies (published)
    public record PaymentHeld(UUID tripId, String holdId) { }
    public record PaymentHoldFailed(UUID tripId, String reason) { }
    public record PaymentReleased(UUID tripId) { }
    public record PaymentCaptured(UUID tripId) { }
    public record PaymentCaptureFailed(UUID tripId, String reason) { }
}
