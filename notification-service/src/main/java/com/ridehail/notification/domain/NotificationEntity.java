package com.ridehail.notification.domain;

import com.ridehail.notification.application.NotificationMessage;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Transient;
import jakarta.persistence.Table;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/** In-app inbox entry. One row per (event, recipient). */
@Entity
@Table(name = "notifications")
public class NotificationEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String userId;

    private UUID eventId;
    private UUID tripId;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false, length = 500)
    private String body;

    private Instant createdAt;
    private Instant readAt;

    @Transient
    private boolean isNewEntity = true;

    protected NotificationEntity() { }

    public static NotificationEntity of(UUID eventId, NotificationMessage m) {
        var n = new NotificationEntity();
        n.id = UUID.randomUUID();
        n.eventId = eventId;
        n.userId = m.userId();
        n.tripId = m.tripId();
        n.type = m.type();
        n.title = m.title();
        n.body = m.body();
        n.createdAt = Instant.now();
        return n;
    }

    public void markRead() {
        if (readAt == null) readAt = Instant.now();
    }

    @Override public UUID getId() { return id; }

    /** True only for entities created in this JVM (not loaded from the DB) => INSERT, not SELECT+INSERT. */
    @Override public boolean isNew() { return isNewEntity; }

    @PostLoad
    @PostPersist
    void markNotNew() { this.isNewEntity = false; }

    public String getUserId() { return userId; }
    public UUID getTripId() { return tripId; }
    public String getType() { return type; }
    public String getTitle() { return title; }
    public String getBody() { return body; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getReadAt() { return readAt; }
}
