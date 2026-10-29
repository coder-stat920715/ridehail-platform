package com.ridehail.notification.api;

import com.ridehail.notification.domain.NotificationEntity;

import java.time.Instant;
import java.util.UUID;

public record NotificationResponse(UUID id, String type, String title, String body, UUID tripId,
                                   Instant createdAt, Instant readAt) {
    public static NotificationResponse from(NotificationEntity n) {
        return new NotificationResponse(n.getId(), n.getType(), n.getTitle(), n.getBody(), n.getTripId(),
                n.getCreatedAt(), n.getReadAt());
    }
}
