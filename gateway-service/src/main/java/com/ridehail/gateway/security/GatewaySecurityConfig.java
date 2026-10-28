package com.ridehail.gateway.security;

import com.ridehail.security.JwtRoleConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * Edge security: stateless JWT validation (issuer-uri => JWKS discovery, signature, exp, iss)
 * plus COARSE RBAC by route. Fine-grained ABAC happens in the owning services, because only they
 * know the trip state/ownership. The JWT is forwarded downstream and re-validated there
 * (zero-trust between tiers - the internal ALB is not a trust boundary).
 */
@Configuration
@EnableWebFluxSecurity
public class GatewaySecurityConfig {

    @Bean
    SecurityWebFilterChain securityFilterChain(ServerHttpSecurity http) {
        return http
                // Bearer-token API, no cookies => no CSRF surface.
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .authorizeExchange(ex -> ex
                        .pathMatchers("/actuator/health/**", "/fallback/**").permitAll()
                        // Service-to-service APIs must never be reachable from the internet.
                        .pathMatchers("/internal/**").denyAll()
                        .pathMatchers(HttpMethod.POST, "/api/v1/trips").hasRole("RIDER")
                        .pathMatchers("/api/v1/drivers/**").hasRole("DRIVER")
                        .anyExchange().authenticated())
                .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(
                        new ReactiveJwtAuthenticationConverterAdapter(JwtRoleConverter.create()))))
                .build();
    }
}
