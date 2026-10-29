package com.ridehail.trip.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

@Configuration
public class LocationClientConfig {

    /** @LoadBalanced: "http://location-service" is resolved through Eureka to a live instance (client-side LB). */
    @Bean
    @LoadBalanced
    RestClient.Builder loadBalancedRestClientBuilder() {
        return RestClient.builder();
    }

    /**
     * Tight transport timeouts are the first line of defence (Resilience4j TimeLimiter / breaker sit above).
     * Service-to-service transport security (mTLS) is provided by the service mesh in Kubernetes (k8s/mesh).
     */
    @Bean
    RestClient locationRestClient(@LoadBalanced RestClient.Builder builder,
                                  @Value("${ridehail.clients.location.base-url}") String baseUrl) {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofMillis(800));
        return builder.baseUrl(baseUrl).requestFactory(factory).build();
    }
}
