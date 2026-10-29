package com.ridehail.notification.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface TripParticipantRepository extends JpaRepository<TripParticipantEntity, UUID> { }
