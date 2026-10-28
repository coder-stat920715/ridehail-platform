package com.ridehail.trip.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Java 21 virtual threads. spring.threads.virtual.enabled=true already moves Tomcat request
 * handling and @KafkaListener containers onto virtual threads; this explicit executor serves
 * our own async fan-out (e.g. the surge call). Pool sizes that still matter: Hikari (DB) and
 * the Resilience4j bulkheads - virtual threads remove thread limits, not downstream limits.
 */
@Configuration
public class VirtualThreadConfig {

    @Bean(destroyMethod = "close")
    ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
