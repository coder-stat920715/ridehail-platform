package com.ridehail.location.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        var backoff = new ExponentialBackOff(200L, 2.0);
        backoff.setMaxInterval(5_000L);
        backoff.setMaxElapsedTime(15_000L);
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backoff);
        handler.addNotRetryableExceptions(JsonProcessingException.class);   // poison pill => straight to DLT
        return handler;
    }
}
