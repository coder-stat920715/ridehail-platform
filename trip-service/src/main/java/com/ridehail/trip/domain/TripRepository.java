package com.ridehail.trip.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.UUID;

public interface TripRepository extends JpaRepository<TripEntity, UUID> {
    boolean existsByRiderIdAndStatusIn(String riderId, Collection<TripStatus> statuses);
}
