package com.ridehail.trip.security;

import com.ridehail.security.JwtRoleConverter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.access.expression.method.MethodSecurityExpressionHandler;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Downstream services re-validate the JWT themselves (zero trust). Method security wires our ABAC
 * evaluator into SpEL so @PreAuthorize("hasPermission(#id,'TRIP','ACCEPT')") works.
 */
@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    SecurityFilterChain apiChain(HttpSecurity http) throws Exception {
        http.csrf(AbstractHttpConfigurer::disable)
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(a -> a
                    .requestMatchers("/actuator/health/**").permitAll()
                    .anyRequest().authenticated())
            .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(JwtRoleConverter.create())));
        return http.build();
    }

    /** static + @Lazy: avoids early-initialising the evaluator's dependencies during post-processor setup. */
    @Bean
    static MethodSecurityExpressionHandler methodSecurityExpressionHandler(
            @Lazy TripPermissionEvaluator evaluator, ApplicationContext context) {
        var handler = new DefaultMethodSecurityExpressionHandler();
        handler.setPermissionEvaluator(evaluator);
        handler.setApplicationContext(context);
        return handler;
    }
}
