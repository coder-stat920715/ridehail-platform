package com.ridehail.notification.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<NotificationEntity, UUID> {
    List<NotificationEntity> findByUserIdOrderByCreatedAtDesc(String userId, Pageable page);
    List<NotificationEntity> findByUserIdAndReadAtIsNullOrderByCreatedAtDesc(String userId, Pageable page);
    long countByUserIdAndReadAtIsNull(String userId);
    Optional<NotificationEntity> findByIdAndUserId(UUID id, String userId);
}
