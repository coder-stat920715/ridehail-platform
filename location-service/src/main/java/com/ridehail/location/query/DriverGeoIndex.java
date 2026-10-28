package com.ridehail.location.query;

import com.ridehail.location.domain.DriverLocationUpdated;
import com.ridehail.location.domain.DriverStatus;
import com.ridehail.location.domain.VehicleType;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * CQRS QUERY side: Redis GEO read model.
 *
 *   geo:drivers:{vehicleType}  GEO (zset)  searchable AVAILABLE drivers only
 *   driver:{id}                HASH        lat,lng,status,vehicleType,ts   (context for ABAC)
 *   drivers:lastseen           ZSET        driverId -> last ping (epoch ms) for stale eviction
 *
 * Lookups are O(N+log M) in-memory GEOSEARCH => typically sub-millisecond server time; the
 * <10ms budget is realistic on same-AZ ElastiCache. GEORADIUS is deprecated since Redis 6.2;
 * GEOSEARCH is its replacement with identical semantics for this use.
 *
 * Scaling: one geo key per vehicle type is a hot key. For multi-city scale shard the key by city or
 * H3/geohash prefix (geo:drivers:{city}:{type}) so Redis Cluster spreads the load.
 */
@Component
public class DriverGeoIndex {

    static final String GEO_PREFIX = "geo:drivers:";
    static final String LASTSEEN = "drivers:lastseen";
    private static final Duration HASH_TTL = Duration.ofHours(1);
    private static final long STALE_AFTER_MS = 30_000;       // no ping for 30s => not searchable

    private final StringRedisTemplate redis;

    public DriverGeoIndex(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Projection of one event. Idempotent (GEOADD overwrites) and guarded against out-of-order delivery. */
    public void apply(DriverLocationUpdated e) {
        String hashKey = "driver:" + e.driverId();
        long ts = e.ts().toEpochMilli();

        Object prev = redis.opsForHash().get(hashKey, "ts");
        if (prev != null && Long.parseLong(prev.toString()) > ts) return;     // stale event

        redis.opsForHash().putAll(hashKey, Map.of(
                "lat", String.valueOf(e.lat()),
                "lng", String.valueOf(e.lng()),
                "status", e.status().name(),
                "vehicleType", e.vehicleType().name(),
                "ts", String.valueOf(ts)));
        redis.expire(hashKey, HASH_TTL);

        String geoKey = GEO_PREFIX + e.vehicleType();
        if (e.status() == DriverStatus.AVAILABLE) {
            redis.opsForGeo().add(geoKey, new Point(e.lng(), e.lat()), e.driverId());   // GEOADD (lng, lat!)
            redis.opsForZSet().add(LASTSEEN, e.driverId(), ts);
        } else {
            removeFromIndex(e.driverId());
        }
    }

    /** "Find active drivers within N km", nearest first. */
    public List<NearbyDriver> nearby(double lat, double lng, double radiusKm, VehicleType type, int limit) {
        GeoResults<GeoLocation<String>> results = redis.opsForGeo().search(
                GEO_PREFIX + type,
                GeoReference.fromCoordinate(lng, lat),
                new Distance(radiusKm, Metrics.KILOMETERS),
                GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().includeCoordinates().sortAscending().limit(limit));

        List<NearbyDriver> out = new ArrayList<>();
        if (results == null) return out;
        results.forEach(r -> out.add(new NearbyDriver(
                r.getContent().getName(),
                r.getDistance().getValue(),
                r.getContent().getPoint().getY(),
                r.getContent().getPoint().getX())));
        return out;
    }

    public int countNearby(double lat, double lng, double radiusKm) {
        int total = 0;
        for (VehicleType t : VehicleType.values()) total += nearby(lat, lng, radiusKm, t, 100).size();
        return total;
    }

    public Optional<DriverContext> context(String driverId) {
        Map<Object, Object> h = redis.opsForHash().entries("driver:" + driverId);
        if (h.isEmpty()) return Optional.empty();
        return Optional.of(new DriverContext(driverId,
                (String) h.get("status"),
                Double.parseDouble((String) h.get("lat")),
                Double.parseDouble((String) h.get("lng")),
                (String) h.get("vehicleType"),
                Long.parseLong((String) h.get("ts"))));
    }

    /** Drivers whose app died never send OFFLINE; a Redis GEO member has no TTL, so we sweep. */
    @Scheduled(fixedDelayString = "PT5S")
    public void evictStale() {
        long cutoff = System.currentTimeMillis() - STALE_AFTER_MS;
        Set<String> stale = redis.opsForZSet().rangeByScore(LASTSEEN, 0, cutoff);
        if (stale == null || stale.isEmpty()) return;
        stale.forEach(this::removeFromIndex);
    }

    private void removeFromIndex(String driverId) {
        for (VehicleType t : VehicleType.values()) redis.opsForGeo().remove(GEO_PREFIX + t, driverId);
        redis.opsForZSet().remove(LASTSEEN, driverId);
    }
}
