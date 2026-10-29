package com.ridehail.eureka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * Service registry. Every other service registers here on startup (name = spring.application.name),
 * sends a heartbeat every 10 s and is evicted after 30 s of silence. The gateway and trip-service
 * resolve "lb://trip-service" / "http://location-service" to live instances from this registry
 * (client-side load balancing via Spring Cloud LoadBalancer).
 *
 * Start this FIRST. For production run >= 2 peers (see k8s/base/eureka-server.yaml) and secure the UI/API.
 */
@SpringBootApplication
@EnableEurekaServer
public class EurekaServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(EurekaServerApplication.class, args);
    }
}
