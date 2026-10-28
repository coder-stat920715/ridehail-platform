# ridehail-platform

Reference implementation of a Rapido-style platform. Read **ARCHITECTURE.md** first.

```
common-outbox/      shared transactional-outbox entity/service/purge job
common-security/    JWT -> ROLE_* mapping (Keycloak realm roles / Cognito groups)
gateway-service/    Spring Cloud Gateway: OIDC resource server, sliding-window limiter, breakers
trip-service/       trips, orchestrated booking saga, ABAC evaluator, Resilience4j clients
location-service/   CQRS: driver location command (PG+outbox) and Redis GEO query + surge
infra/              Debezium connector JSONs, register script, Kafka topic/command contract
```

Build: `mvn -q clean verify` (JDK 21). Run order: Postgres (wal_level=logical) → Kafka + Kafka Connect → `infra/register-connectors.sh`
→ Redis → services. Required env: `OIDC_ISSUER_URI`, `TRIP_DB_URL`, `LOCATION_DB_URL`, `KAFKA_BOOTSTRAP`, `REDIS_HOST`, `INTERNAL_ALB_URI`.

## Testing
`postman/` contains the Postman collection, environment and **TESTING_GUIDE.md** (step-by-step).
`infra/local/` contains a Docker Compose stack (Postgres, Redis, Kafka, Debezium, Kafka REST, Keycloak with seeded users) and scripts to run it.
