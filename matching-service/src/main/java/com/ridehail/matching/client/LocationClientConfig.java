package com.ridehail.matching.client;

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

    /** "http://location-service" is a logical Eureka name resolved by client-side load balancing. */
    @Bean
    @LoadBalanced
    RestClient.Builder loadBalancedRestClientBuilder() {
        return RestClient.builder();
    }

    @Bean
    RestClient locationRestClient(@LoadBalanced RestClient.Builder builder,
                                  @Value("${ridehail.clients.location.base-url}") String baseUrl) {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofMillis(1000));
        return builder.baseUrl(baseUrl).requestFactory(factory).build();
    }
}
