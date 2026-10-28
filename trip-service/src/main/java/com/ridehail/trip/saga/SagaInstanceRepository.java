package com.ridehail.trip.saga;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface SagaInstanceRepository extends JpaRepository<SagaInstanceEntity, UUID> {

    /**
     * SELECT ... FOR UPDATE. Serialises Kafka replies and REST driver/rider actions for the same
     * trip so two concurrent events can't both pass a state check.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from SagaInstanceEntity s where s.tripId = :id")
    Optional<SagaInstanceEntity> lockById(@Param("id") UUID id);
}
