package com.ridehail.trip.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

@Configuration
public class LocationClientConfig {

    /**
     * Tight transport timeouts: the first line of defence (Resilience4j TimeLimiter / breaker sit above).
     * Internal calls go via the internal ALB. In production add mTLS or client-credentials tokens.
     */
    @Bean
    RestClient locationRestClient(@Value("${ridehail.clients.location.base-url}") String baseUrl) {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofMillis(800));
        return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }
}
