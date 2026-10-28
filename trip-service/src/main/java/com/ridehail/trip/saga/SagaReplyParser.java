package com.ridehail.trip.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class SagaReplyParser {

    private final ObjectMapper mapper;

    public SagaReplyParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** Unknown event types on shared topics are not ours => ignored (not dead-lettered). */
    public Optional<SagaReply> parse(String eventType, String payload) throws JsonProcessingException {
        return Optional.ofNullable(switch (eventType) {
            case "PAYMENT_HELD" -> mapper.readValue(payload, SagaReply.PaymentHeld.class);
            case "PAYMENT_HOLD_FAILED" -> mapper.readValue(payload, SagaReply.PaymentHoldFailed.class);
            case "DRIVER_MATCHED" -> mapper.readValue(payload, SagaReply.DriverMatched.class);
            case "DRIVER_MATCH_FAILED" -> mapper.readValue(payload, SagaReply.DriverMatchFailed.class);
            case "PAYMENT_RELEASED" -> mapper.readValue(payload, SagaReply.PaymentReleased.class);
            case "PAYMENT_CAPTURED" -> mapper.readValue(payload, SagaReply.PaymentCaptured.class);
            case "PAYMENT_CAPTURE_FAILED" -> mapper.readValue(payload, SagaReply.PaymentCaptureFailed.class);
            default -> null;
        });
    }
}
