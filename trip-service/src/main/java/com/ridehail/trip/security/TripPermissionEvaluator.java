package com.ridehail.trip.security;

import com.ridehail.trip.client.DriverContext;
import com.ridehail.trip.client.DriverContextClient;
import com.ridehail.trip.domain.GeoMath;
import com.ridehail.trip.domain.TripEntity;
import com.ridehail.trip.domain.TripRepository;
import com.ridehail.trip.domain.TripStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * ABAC policy decision point for trips.
 *
 * Attributes evaluated:
 *   subject  : user id (JWT sub), roles
 *   resource : trip owner, assigned driver, trip state
 *   context  : driver availability and live geolocation (from the location read-model)
 *
 * Fails CLOSED: unknown trip, malformed input, unknown action or an unavailable context service
 * all result in "deny". Role checks (RBAC) are layered on top in @PreAuthorize.
 */
@Component
public class TripPermissionEvaluator implements PermissionEvaluator {

    private static final Logger log = LoggerFactory.getLogger(TripPermissionEvaluator.class);

    private static final double ACCEPT_RADIUS_KM = 3.0;   // driver must still be near the pickup to accept
    private static final double START_RADIUS_KM = 0.3;    // driver must be at the pickup to start
    private static final Set<String> ACCEPTING_STATUSES = Set.of("AVAILABLE", "OFFERED");
    private static final Set<TripStatus> CANCELLABLE = EnumSet.of(TripStatus.REQUESTED, TripStatus.PAYMENT_HELD,
            TripStatus.DRIVER_OFFERED, TripStatus.DRIVER_ASSIGNED);

    private final TripRepository trips;
    private final DriverContextClient drivers;

    public TripPermissionEvaluator(TripRepository trips, DriverContextClient drivers) {
        this.trips = trips;
        this.drivers = drivers;
    }

    /** Domain-object form is not used: always deny. */
    @Override
    public boolean hasPermission(Authentication auth, Object target, Object permission) {
        return false;
    }

    @Override
    @Transactional(readOnly = true)
    public boolean hasPermission(Authentication auth, Serializable targetId, String targetType, Object permission) {
        if (auth == null || !auth.isAuthenticated() || !"TRIP".equals(targetType)) return false;

        final TripAction action;
        final UUID tripId;
        try {
            action = TripAction.valueOf(String.valueOf(permission));
            tripId = UUID.fromString(String.valueOf(targetId));
        } catch (IllegalArgumentException e) {
            return false;
        }
        // Missing trip => deny (403) rather than 404, so ids can't be probed.
        TripEntity trip = trips.findById(tripId).orElse(null);
        if (trip == null) return false;

        var subject = Subject.from(auth);
        boolean allowed = evaluate(action, subject, trip);
        if (!allowed) log.debug("ABAC deny: user={} action={} trip={} status={}", subject.id(), action, tripId, trip.getStatus());
        return allowed;
    }

    private boolean evaluate(TripAction action, Subject s, TripEntity trip) {
        boolean participant = trip.isOwnedBy(s.id()) || trip.isAssignedTo(s.id());
        return switch (action) {
            case VIEW -> s.hasRole("ADMIN") || participant;

            case CANCEL -> (s.hasRole("ADMIN") || participant) && CANCELLABLE.contains(trip.getStatus());

            case ACCEPT -> s.hasRole("DRIVER")
                    && trip.isAssignedTo(s.id())
                    && trip.getStatus() == TripStatus.DRIVER_OFFERED
                    && drivers.find(s.id())
                        .filter(c -> ACCEPTING_STATUSES.contains(c.status()))
                        .map(c -> within(c, trip, ACCEPT_RADIUS_KM))
                        .orElse(false);

            case START -> s.hasRole("DRIVER")
                    && trip.isAssignedTo(s.id())
                    && trip.getStatus() == TripStatus.DRIVER_ASSIGNED
                    && drivers.find(s.id()).map(c -> within(c, trip, START_RADIUS_KM)).orElse(false);

            case COMPLETE -> s.hasRole("DRIVER")
                    && trip.isAssignedTo(s.id())
                    && trip.getStatus() == TripStatus.IN_PROGRESS;
        };
    }

    private static boolean within(DriverContext c, TripEntity trip, double radiusKm) {
        return GeoMath.haversineKm(c.lat(), c.lng(), trip.pickup().lat(), trip.pickup().lng()) <= radiusKm;
    }

    private record Subject(String id, Set<String> roles) {
        static Subject from(Authentication auth) {
            Set<String> roles = auth.getAuthorities().stream()
                    .map(GrantedAuthority::getAuthority)
                    .filter(a -> a.startsWith("ROLE_"))
                    .map(a -> a.substring(5))
                    .collect(Collectors.toUnmodifiableSet());
            return new Subject(auth.getName(), roles);
        }
        boolean hasRole(String r) { return roles.contains(r); }
    }
}
