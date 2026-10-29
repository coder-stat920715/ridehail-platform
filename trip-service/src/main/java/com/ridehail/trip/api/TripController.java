package com.ridehail.trip.api;

import com.ridehail.trip.api.dto.BookRideRequest;
import com.ridehail.trip.api.dto.CancelRequest;
import com.ridehail.trip.api.dto.CompleteTripRequest;
import com.ridehail.trip.api.dto.TripResponse;
import com.ridehail.trip.application.TripCommandService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

/**
 * Thin controller: authN/authZ + DTO mapping only. Authorization is declarative and two-layered:
 *   hasRole(...)                       -> RBAC  (who are you)
 *   hasPermission(id,'TRIP',ACTION)    -> ABAC  (is THIS action allowed on THIS trip right now)
 *
 * Note: Spring's SpEL root supplies the Authentication implicitly, so the 3-arg
 * hasPermission(targetId, targetType, permission) form reaches
 * PermissionEvaluator.hasPermission(authentication, targetId, targetType, permission).
 */
@RestController
@RequestMapping("/api/v1/trips")
public class TripController {

    private final TripCommandService service;

    public TripController(TripCommandService service) {
        this.service = service;
    }

    /** 202 Accepted: booking is asynchronous - the saga continues in the background. */
    @PostMapping
    @PreAuthorize("hasRole('RIDER')")
    public ResponseEntity<TripResponse> book(@Valid @RequestBody BookRideRequest req, @AuthenticationPrincipal Jwt jwt) {
        String name = jwt.getClaimAsString("preferred_username");
        TripResponse trip = service.bookRide(jwt.getSubject(), name != null ? name : jwt.getSubject(), req);
        return ResponseEntity.accepted().location(URI.create("/api/v1/trips/" + trip.tripId())).body(trip);
    }

    @GetMapping("/{tripId}")
    @PreAuthorize("hasPermission(#tripId, 'TRIP', 'VIEW')")
    public TripResponse get(@PathVariable UUID tripId) {
        return service.get(tripId);
    }

    @PostMapping("/{tripId}/accept")
    @PreAuthorize("hasRole('DRIVER') and hasPermission(#tripId, 'TRIP', 'ACCEPT')")
    public TripResponse accept(@PathVariable UUID tripId, @AuthenticationPrincipal Jwt jwt) {
        return service.accept(tripId, jwt.getSubject());
    }

    @PostMapping("/{tripId}/start")
    @PreAuthorize("hasRole('DRIVER') and hasPermission(#tripId, 'TRIP', 'START')")
    public TripResponse start(@PathVariable UUID tripId, @AuthenticationPrincipal Jwt jwt) {
        return service.start(tripId, jwt.getSubject());
    }

    @PostMapping("/{tripId}/complete")
    @PreAuthorize("hasRole('DRIVER') and hasPermission(#tripId, 'TRIP', 'COMPLETE')")
    public TripResponse complete(@PathVariable UUID tripId, @Valid @RequestBody CompleteTripRequest req,
                                 @AuthenticationPrincipal Jwt jwt) {
        return service.complete(tripId, jwt.getSubject(), req.distanceKm());
    }

    @PostMapping("/{tripId}/cancel")
    @PreAuthorize("hasPermission(#tripId, 'TRIP', 'CANCEL')")
    public TripResponse cancel(@PathVariable UUID tripId, @RequestBody(required = false) @Valid CancelRequest req) {
        return service.cancel(tripId, req == null ? null : req.reason());
    }
}
