package com.ridehail.notification.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.ridehail.notification.domain.TripParticipantEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * WHO gets WHAT on WHICH channel for each trip event. Pure function of (event, participants): easy to unit test and
 * the only place to touch when copy or routing changes. Payloads are read leniently (JsonNode) so new fields added
 * by trip-service never break this consumer ("tolerant reader").
 */
@Component
public class NotificationPolicy {

    private static final Logger log = LoggerFactory.getLogger(NotificationPolicy.class);
    private static final Set<Channel> PUSH = EnumSet.of(Channel.PUSH);
    private static final Set<Channel> PUSH_SMS = EnumSet.of(Channel.PUSH, Channel.SMS);
    private static final Set<Channel> NONE = EnumSet.noneOf(Channel.class);

    public List<NotificationMessage> build(String type, JsonNode e, TripParticipantEntity p) {
        if (p == null) {
            log.warn("No participants known for event {} (trip started before this service?) - skipping", type);
            return List.of();
        }
        UUID trip = p.getTripId();
        String rider = p.getRiderId();
        String driver = p.getDriverId();
        List<NotificationMessage> out = new ArrayList<>();

        switch (type) {
            case "TRIP_REQUESTED" ->
                out.add(msg(rider, type, "Finding your ride", "We are looking for a nearby driver.", trip, NONE));

            case "DRIVER_OFFERED" -> {
                int mins = (int) Math.ceil(e.path("etaSeconds").asInt(0) / 60.0);
                if (driver != null) {
                    out.add(msg(driver, type, "New ride request",
                            "A rider is waiting about " + mins + " min away. Open the app to accept.", trip, PUSH));
                }
                out.add(msg(rider, type, "Driver found",
                        "A driver about " + mins + " min away is confirming your ride.", trip, NONE));
            }
            case "DRIVER_ASSIGNED" ->
                out.add(msg(rider, type, "Driver is on the way", "Your driver accepted the ride.", trip, PUSH));

            case "TRIP_STARTED" ->
                out.add(msg(rider, type, "Trip started", "Enjoy your ride!", trip, NONE));

            case "TRIP_COMPLETED" -> {
                String fare = e.path("finalFare").asText("");
                out.add(msg(rider, type, "Trip completed",
                        fare.isEmpty() ? "Thanks for riding with us." : "Your fare is Rs. " + fare + ".", trip, PUSH_SMS));
                if (driver != null) out.add(msg(driver, type, "Trip completed", "Good job! The trip has ended.", trip, NONE));
            }
            case "TRIP_CANCELLED" -> {
                String reason = e.path("reason").asText("no reason given");
                out.add(msg(rider, type, "Trip cancelled", "Your trip was cancelled: " + reason, trip, PUSH));
                if (driver != null) out.add(msg(driver, type, "Trip cancelled", "The trip was cancelled.", trip, PUSH));
            }
            case "PAYMENT_CAPTURE_FAILED" ->
                out.add(msg(rider, type, "Payment failed",
                        "We could not charge your payment method. Please update it to avoid a block.", trip, PUSH_SMS));

            default -> { /* event types we do not notify on */ }
        }
        return out;
    }

    private static NotificationMessage msg(String user, String type, String title, String body, UUID trip, Set<Channel> ch) {
        return new NotificationMessage(user, type, title, body, trip, ch);
    }
}
