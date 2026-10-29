package com.ridehail.matching.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    /** Retry with exponential backoff (covers location-service outages), then <topic>.DLT. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        var backoff = new ExponentialBackOff(500L, 2.0);
        backoff.setMaxInterval(10_000L);
        backoff.setMaxElapsedTime(45_000L);
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backoff);
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        return handler;
    }
}
