package com.ridehail.payment.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ridehail.outbox.OutboxService;
import com.ridehail.payment.application.PaymentMessages.*;
import com.ridehail.payment.domain.PaymentEntity;
import com.ridehail.payment.domain.PaymentRepository;
import com.ridehail.payment.domain.PaymentState;
import com.ridehail.payment.domain.ProcessedEventEntity;
import com.ridehail.payment.domain.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Saga participant logic. Each command = ONE local transaction:
 *   [idempotency ledger + payment state change + outbox reply]
 * so a reply is published if and only if the state change committed (same outbox pattern as trip-service).
 */
@Service
public class PaymentCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandHandler.class);
    private static final String REPLY_AGGREGATE = "payment";      // => topic outbox.event.payment

    private final PaymentRepository payments;
    private final ProcessedEventRepository processed;
    private final SimulatedPaymentGateway gateway;
    private final OutboxService outbox;
    private final ObjectMapper mapper;

    public PaymentCommandHandler(PaymentRepository payments, ProcessedEventRepository processed,
                                 SimulatedPaymentGateway gateway, OutboxService outbox, ObjectMapper mapper) {
        this.payments = payments;
        this.processed = processed;
        this.gateway = gateway;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    @Transactional
    public void handle(UUID eventId, String commandType, String payload) throws JsonProcessingException {
        if (processed.existsById(eventId)) {
            log.debug("Duplicate command {} ignored", eventId);
            return;
        }
        processed.save(ProcessedEventEntity.of(eventId));

        switch (commandType) {
            case "HOLD_PAYMENT" -> hold(mapper.readValue(payload, HoldPayment.class));
            case "CAPTURE_PAYMENT" -> capture(mapper.readValue(payload, CapturePayment.class));
            case "RELEASE_PAYMENT_HOLD" -> release(mapper.readValue(payload, ReleasePaymentHold.class));
            default -> log.debug("Ignoring command type {}", commandType);
        }
    }

    private void hold(HoldPayment cmd) {
        if (payments.existsById(cmd.tripId())) return;               // replayed command for the same trip
        var payment = PaymentEntity.create(cmd.tripId(), cmd.riderId(), cmd.riderName(), cmd.amount());
        var result = gateway.authorize(cmd.riderName(), cmd.amount());
        if (result.approved()) {
            payment.held(result.reference());
            payments.save(payment);
            reply(cmd.tripId(), "PAYMENT_HELD", new PaymentHeld(cmd.tripId(), result.reference()));
        } else {
            payment.holdFailed(result.reason());
            payments.save(payment);
            log.warn("Hold DECLINED for trip {} rider '{}': {}", cmd.tripId(), cmd.riderName(), result.reason());
            reply(cmd.tripId(), "PAYMENT_HOLD_FAILED", new PaymentHoldFailed(cmd.tripId(), result.reason()));
        }
    }

    private void capture(CapturePayment cmd) {
        var payment = payments.findById(cmd.tripId()).orElse(null);
        if (payment == null || payment.getState() != PaymentState.HELD && payment.getState() != PaymentState.CAPTURED) {
            reply(cmd.tripId(), "PAYMENT_CAPTURE_FAILED", new PaymentCaptureFailed(cmd.tripId(), "NO_ACTIVE_HOLD"));
            return;
        }
        if (payment.getState() == PaymentState.CAPTURED) {           // idempotent re-answer
            reply(cmd.tripId(), "PAYMENT_CAPTURED", new PaymentCaptured(cmd.tripId()));
            return;
        }
        var result = gateway.capture(payment.getRiderName(), cmd.amount());
        if (result.approved()) {
            payment.captured();
            reply(cmd.tripId(), "PAYMENT_CAPTURED", new PaymentCaptured(cmd.tripId()));
        } else {
            payment.captureFailed(result.reason());
            log.warn("Capture REJECTED for trip {}: {}", cmd.tripId(), result.reason());
            reply(cmd.tripId(), "PAYMENT_CAPTURE_FAILED", new PaymentCaptureFailed(cmd.tripId(), result.reason()));
        }
    }

    /** Compensation. Always answers PAYMENT_RELEASED (even if nothing was held) so the saga can finish. */
    private void release(ReleasePaymentHold cmd) {
        payments.findById(cmd.tripId()).ifPresent(p -> {
            if (p.getState() == PaymentState.HELD) p.released();
        });
        reply(cmd.tripId(), "PAYMENT_RELEASED", new PaymentReleased(cmd.tripId()));
    }

    private void reply(UUID tripId, String type, Object payload) {
        outbox.publish(REPLY_AGGREGATE, tripId.toString(), type, payload);
    }
}
