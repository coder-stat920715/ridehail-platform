package com.ridehail.trip.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    /**
     * Poison-pill protection: retry with exponential backoff, then publish to <topic>.DLT and move
     * on, so one bad message can't block a partition. DLT topics need the same partition count.
     * Spring Boot auto-applies this CommonErrorHandler bean to @KafkaListener containers.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        var backoff = new ExponentialBackOff(500L, 2.0);
        backoff.setMaxInterval(10_000L);
        backoff.setMaxElapsedTime(30_000L);
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backoff);
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        return handler;
    }
}
