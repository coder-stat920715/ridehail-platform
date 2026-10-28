package com.ridehail.trip.resilience;

import io.github.resilience4j.common.retry.configuration.RetryConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class ResilienceConfig {

    /** Full-jitter backoff for the "driverContext" retry instance (other params come from YAML). */
    @Bean
    RetryConfigCustomizer driverContextRetryCustomizer() {
        return RetryConfigCustomizer.of("driverContext", builder ->
                builder.intervalFunction(new FullJitterIntervalFunction(
                        Duration.ofMillis(100), 2.0, Duration.ofSeconds(2))));
    }
}
