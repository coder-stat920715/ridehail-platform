package com.ridehail.gateway.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.ratelimit.AbstractRateLimiter;
import org.springframework.cloud.gateway.support.ConfigurationService;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Redis-backed SLIDING-WINDOW-LOG rate limiter.
 *
 * Spring Cloud Gateway's built-in RedisRateLimiter is a token bucket, which allows 2x bursts at
 * bucket boundaries; a sliding window enforces "at most N requests in ANY window of W seconds".
 * Implemented as one atomic Lua script on a ZSET (score = timestamp ms) => one Redis round trip,
 * no race between count and add.
 *
 * Route config (application.yml):
 *   sliding-window-rate-limiter.limit: 20
 *   sliding-window-rate-limiter.windowSeconds: 10
 *
 * FAIL-OPEN decision: if Redis is down we allow traffic. Availability of the booking path matters
 * more than perfect limiting; AWS WAF rate-based rules remain the hard outer limit.
 */
@Component("slidingWindowRateLimiter")
@SuppressWarnings({"rawtypes", "unchecked"})
public class SlidingWindowRateLimiter extends AbstractRateLimiter<SlidingWindowRateLimiter.Config> {

    public static final String CONFIGURATION_PROPERTY_NAME = "sliding-window-rate-limiter";
    private static final Logger log = LoggerFactory.getLogger(SlidingWindowRateLimiter.class);

    private static final String LUA = """
            local key    = KEYS[1]
            local now    = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit  = tonumber(ARGV[3])
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            local count = redis.call('ZCARD', key)
            if count < limit then
              redis.call('ZADD', key, now, ARGV[4])
              redis.call('PEXPIRE', key, window)
              return {1, limit - count - 1}
            end
            return {0, 0}
            """;
    private static final DefaultRedisScript<List> SCRIPT = new DefaultRedisScript<>(LUA, List.class);

    private final ReactiveStringRedisTemplate redis;

    public SlidingWindowRateLimiter(ReactiveStringRedisTemplate redis, ConfigurationService configurationService) {
        super(Config.class, CONFIGURATION_PROPERTY_NAME, configurationService);
        this.redis = redis;
    }

    @Override
    public Mono<Response> isAllowed(String routeId, String id) {
        Config cfg = getConfig().get(routeId);
        if (cfg == null) cfg = new Config();
        final Config c = cfg;

        long now = System.currentTimeMillis();
        String key = "rl:sw:" + routeId + ":" + id;
        List<String> args = List.of(
                String.valueOf(now),
                String.valueOf(c.getWindowSeconds() * 1000L),
                String.valueOf(c.getLimit()),
                now + "-" + UUID.randomUUID());      // unique member so same-ms requests don't collapse

        return redis.execute(SCRIPT, List.of(key), args)
                .next()
                .map(raw -> {
                    List<Long> r = (List<Long>) raw;
                    boolean allowed = r.get(0) == 1L;
                    return new Response(allowed, Map.of(
                            "X-RateLimit-Limit", String.valueOf(c.getLimit()),
                            "X-RateLimit-Remaining", String.valueOf(r.get(1))));
                })
                .onErrorResume(ex -> {
                    log.error("Rate limiter Redis failure, failing open: {}", ex.toString());
                    return Mono.just(new Response(true, Map.of()));
                });
    }

    public static class Config {
        private int limit = 20;
        private int windowSeconds = 10;

        public int getLimit() { return limit; }
        public void setLimit(int limit) { this.limit = limit; }
        public int getWindowSeconds() { return windowSeconds; }
        public void setWindowSeconds(int windowSeconds) { this.windowSeconds = windowSeconds; }
    }
}
