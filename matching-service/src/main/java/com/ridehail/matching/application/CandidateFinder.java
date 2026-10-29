package com.ridehail.matching.application;

import com.ridehail.matching.application.MatchMessages.MatchDriver;
import com.ridehail.matching.client.DriverCandidate;
import com.ridehail.matching.client.DriverSearchClient;
import com.ridehail.matching.domain.MatchRequestRepository;
import com.ridehail.matching.domain.MatchState;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Finds free drivers around the pickup, widening the search radius step by step (3 km -> 6 km -> 10 km, the maximum
 * location-service accepts). Runs OUTSIDE any DB transaction because it performs remote calls.
 */
@Component
public class CandidateFinder {

    private static final double MAX_RADIUS_KM = 10.0;
    private static final int LIMIT = 20;

    public record SearchResult(List<DriverCandidate> candidates, double radiusKm) { }

    private final DriverSearchClient search;
    private final MatchRequestRepository requests;

    public CandidateFinder(DriverSearchClient search, MatchRequestRepository requests) {
        this.search = search;
        this.requests = requests;
    }

    public SearchResult find(MatchDriver cmd) {
        // Drivers already holding an offer/assignment for ANOTHER trip must not be offered again.
        Set<String> reserved = new HashSet<>(requests.findReservedDrivers(MatchState.RESERVING));

        double last = Math.min(cmd.radiusKm(), MAX_RADIUS_KM);
        for (double radius : steps(cmd.radiusKm())) {
            last = radius;
            List<DriverCandidate> free = search
                    .nearby(cmd.pickup().lat(), cmd.pickup().lng(), cmd.vehicleType(), radius, LIMIT).stream()
                    .filter(c -> !reserved.contains(c.driverId()))
                    .toList();
            if (!free.isEmpty()) return new SearchResult(free, radius);
        }
        return new SearchResult(List.of(), last);
    }

    private static Set<Double> steps(double requested) {
        Set<Double> s = new TreeSet<>();
        s.add(Math.min(requested, MAX_RADIUS_KM));
        s.add(Math.min(Math.max(requested * 2, 6.0), MAX_RADIUS_KM));
        s.add(MAX_RADIUS_KM);
        return s;
    }
}
