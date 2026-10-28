package com.ridehail.location.command;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/drivers/me")
public class LocationCommandController {

    private final LocationCommandService service;

    public LocationCommandController(LocationCommandService service) {
        this.service = service;
    }

    /**
     * The driver id comes from the verified JWT, never from the body: a driver can only move
     * their own pin. 202 because the read model updates asynchronously (eventual consistency).
     */
    @PutMapping("/location")
    @PreAuthorize("hasRole('DRIVER')")
    public ResponseEntity<Void> update(@Valid @RequestBody LocationUpdateRequest req,
                                       @AuthenticationPrincipal Jwt jwt) {
        service.record(jwt.getSubject(), req);
        return ResponseEntity.accepted().build();
    }
}
