package com.ridehail.notification.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ridehail.notification.domain.NotificationEntity;
import com.ridehail.notification.domain.NotificationRepository;
import com.ridehail.notification.domain.ProcessedEventEntity;
import com.ridehail.notification.domain.ProcessedEventRepository;
import com.ridehail.notification.domain.TripParticipantEntity;
import com.ridehail.notification.domain.TripParticipantRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * One transaction per event: [idempotency ledger + participant read model + inbox rows].
 * External channels (push/SMS) are NOT called here - slow third-party calls must never hold a DB transaction; the
 * listener delivers them AFTER this transaction committed (see TripEventListener).
 */
@Service
public class NotificationService {

    private final ProcessedEventRepository processed;
    private final TripParticipantRepository participants;
    private final NotificationRepository notifications;
    private final NotificationPolicy policy;
    private final ObjectMapper mapper;

    public NotificationService(ProcessedEventRepository processed, TripParticipantRepository participants,
                               NotificationRepository notifications, NotificationPolicy policy, ObjectMapper mapper) {
        this.processed = processed;
        this.participants = participants;
        this.notifications = notifications;
        this.policy = policy;
        this.mapper = mapper;
    }

    /** @return the messages that still need EXTERNAL delivery (empty for duplicates / unknown events). */
    @Transactional
    public List<NotificationMessage> process(UUID eventId, String type, String payload) throws JsonProcessingException {
        if (processed.existsById(eventId)) return List.of();
        processed.save(ProcessedEventEntity.of(eventId));

        JsonNode event = mapper.readTree(payload);
        UUID tripId = UUID.fromString(event.path("tripId").asText());
        TripParticipantEntity participant = updateReadModel(type, event, tripId);

        List<NotificationMessage> messages = policy.build(type, event, participant);
        for (NotificationMessage m : messages) {
            notifications.save(NotificationEntity.of(eventId, m));
        }
        return messages.stream().filter(m -> !m.externalChannels().isEmpty()).toList();
    }

    private TripParticipantEntity updateReadModel(String type, JsonNode event, UUID tripId) {
        if ("TRIP_REQUESTED".equals(type)) {
            return participants.save(TripParticipantEntity.forRider(tripId, event.path("riderId").asText()));
        }
        Optional<TripParticipantEntity> existing = participants.findById(tripId);
        existing.ifPresent(p -> {
            if (("DRIVER_OFFERED".equals(type) || "DRIVER_ASSIGNED".equals(type)) && event.hasNonNull("driverId")) {
                p.assignDriver(event.get("driverId").asText());
            }
        });
        return existing.orElse(null);
    }

    // ---------------------------------------------------------------- inbox queries (REST API)
    @Transactional(readOnly = true)
    public List<NotificationEntity> inbox(String userId, boolean unreadOnly, int limit) {
        var page = PageRequest.of(0, Math.max(1, Math.min(limit, 100)));
        return unreadOnly
                ? notifications.findByUserIdAndReadAtIsNullOrderByCreatedAtDesc(userId, page)
                : notifications.findByUserIdOrderByCreatedAtDesc(userId, page);
    }

    @Transactional(readOnly = true)
    public long unreadCount(String userId) {
        return notifications.countByUserIdAndReadAtIsNull(userId);
    }

    /** @return false if the notification does not exist OR belongs to someone else (indistinguishable on purpose). */
    @Transactional
    public boolean markRead(UUID id, String userId) {
        return notifications.findByIdAndUserId(id, userId).map(n -> { n.markRead(); return true; }).orElse(false);
    }
}
