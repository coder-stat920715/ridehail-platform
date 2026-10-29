# Ridehail – Postman Testing Guide

Files in this folder

| File | Purpose |
|---|---|
| `Ridehail.postman_collection.json` | 123 requests in 13 folders, each with automated assertions |
| `Ridehail-Local.postman_environment.json` | Local URLs, test users, coordinates |

The collection needs a running stack. `infra/local/` contains a Docker Compose file with everything except the three Spring Boot services.

---

## 1. Why some requests say "⚙ SIMULATE"

`payment-service` and `matching-service` are **not implemented** in this repo (contract only). The booking saga would stall after the first command, so those requests **produce the reply events to Kafka** the way those services would, using the Confluent Kafka REST Proxy (v3 API):

* topic `outbox.event.payment` / `outbox.event.matching`
* headers `id` (event UUID → idempotency key) and `eventType`, exactly like Debezium's EventRouter
* key = tripId, value = JSON payload

Everything else (gateway, JWT, RBAC, ABAC, outbox, Debezium, saga, CQRS, Redis) is the real code path.

---

## 2. One-time setup

### 2.1 Prerequisites
JDK 21, Maven 3.9+, Docker + Docker Compose, Postman (or Newman), `curl`, `python3`.

### 2.2 Start infrastructure
```bash
cd infra/local
docker compose up -d
./create-topics.sh              # pre-creates topics (+ .DLT); the REST proxy won't produce to missing topics
```
Components and host ports: PostgreSQL 5432 (`wal_level=logical`), Redis 6379, Kafka 9092, Kafka Connect/Debezium 8083, Kafka REST 8090, Keycloak 8180.

Keycloak imports realm **`ridehail`** automatically (client `ridehail-app`, password grant enabled). Users, all with password `Passw0rd!`:

| User | Role |
|---|---|
| rider1, rider2 | RIDER |
| FAIL_rider | RIDER (name starts with FAIL → payment declined) |
| CAPFAIL_rider | RIDER (name starts with CAPFAIL → capture rejected) |
| driver1, driver2 | DRIVER |
| admin1 | ADMIN |

Wait ~30 s, then check `http://localhost:8180/realms/ridehail/.well-known/openid-configuration` returns JSON.

### 2.3 Build, then start the services IN THIS ORDER (one terminal each)
```bash
mvn -q -DskipTests install          # repo root: builds every module once
```
| # | Service | Command | Why in this position |
|---|---|---|---|
| 1 | **eureka-server** :8761 | `mvn -pl eureka-server spring-boot:run` | Registry first: everyone else registers with it and resolves peers through it. Check http://localhost:8761 |
| 2 | **location-service** :8082 | `mvn -pl location-service spring-boot:run` | A dependency of trip-service (surge + driver context); creates its DB tables (Flyway) |
| 3 | **trip-service** :8081 | `mvn -pl trip-service spring-boot:run` | Needs Eureka (to find `location-service`), Kafka, DB; creates its tables |
| 4 | **payment-service** :8084 *(Mode B/C)* | `mvn -pl payment-service spring-boot:run` | Saga participant; creates its tables |
| 4b | **matching-service** :8085 *(Mode C)* | `mvn -pl matching-service spring-boot:run` | Needs Eureka (finds `location-service`) and Kafka; creates its tables |
| 4c | **notification-service** :8086 *(Mode C)* | `mvn -pl notification-service spring-boot:run` | Pure consumer of `outbox.event.trip`; creates its tables |
| 5 | *(register Debezium, see 2.4)* | | Needs the `outbox` tables from steps 2–4 |
| 6 | **gateway-service** :8080 | `mvn -pl gateway-service spring-boot:run` | Last: routes `lb://trip-service` / `lb://location-service` only work once they are registered |

Open http://localhost:8761: you should see `LOCATION-SERVICE`, `TRIP-SERVICE` (`PAYMENT-SERVICE`, `GATEWAY-SERVICE` once started) as UP.
Defaults already match the local stack (DB users `trip/trip`, `location/location`, `payment/payment`; Kafka `localhost:9092`; Keycloak issuer `http://localhost:8180/realms/ridehail`; Eureka `http://localhost:8761/eureka/`).

### 2.4 Register the Debezium connectors
Only **after** the services of steps 2–4 started once (Flyway creates the `outbox` tables):
```bash
infra/local/register-local.sh                      # all four (trip, location, payment, matching)
infra/local/register-local.sh trip location        # Mode A (payment-service not running)
```
Expected: `trip-outbox-connector RUNNING ['RUNNING']` (and location / payment).

### 2.5 Two test modes
| Mode | payment-service | Folders to run | Payment replies come from |
|---|---|---|---|
| **A – simulated participants** | not running | 0 → 8 (then 9, 10 manually) | Postman "⚙ SIMULATE" requests |
| **B – real payment-service** | running + connector registered; **matching-service must NOT run** | 0, 1, 2, then **11** | the real service (FAIL / CAPFAIL rules); matching still simulated |
| **C – everything real** | payment, matching and notification services running; trip/location/payment/matching connectors registered | 0, 1, 2, then **12** | real services only (real driver search + reservation, offer expiry, notification inbox) |

Do not mix: with a real payment-service running, simulated payment replies arrive *after* the real ones and are (correctly) ignored as stale, so folders 4–6 would fail their assertions.

### 2.6 Import into Postman
1. *Import* → both JSON files.
2. Select environment **Ridehail – Local**.
3. Don't edit variables unless your ports differ. Tokens and ids are filled in automatically by test scripts (collection variables).

---

## 3. Running the tests

### Option A – manual (recommended the first time)
Open each folder and send requests top to bottom. Read the request *Description* tab: it says what is being proven. Open the **Console** (View → Show Postman Console) to see the simulated event ids and rate-limit headers.

### Option B – Collection Runner
Run folders **0 → 8** in order. Keep "Persist variables" on, **delay 0**, iterations 1. Do **not** include folders 9 and 10 (they need manual start/stop of processes). For folder 8 run only request 8.1 with **30 iterations**.

### Option C – Newman (CI)
```bash
cd postman
newman run Ridehail.postman_collection.json -e Ridehail-Local.postman_environment.json \
  --folder "0 · Setup & smoke tests" --folder "1 · Edge security (negative tests)" \
  --folder "2 · Location service (CQRS command → Debezium → Redis read model)" \
  --folder "3 · Happy path: book → hold → match → accept → start → complete → capture" \
  --folder "4 · Compensation A: payment hold fails" \
  --folder "5 · Compensation B: no driver found → release payment hold" \
  --folder "6 · Rider-initiated cancellation" \
  --folder "7 · Idempotency & out-of-order protection"
```
Run folders in one invocation so collection variables carry over.

> Timing: read-after-write requests have a built-in pre-request delay (`propagation_delay_ms`, default 2000 ms) to allow Postgres → Debezium → Kafka → consumer. On a slow laptop raise it to 3000–4000.

---

## 4. Detailed walkthrough and what to verify behind the scenes

### Folder 0 – Setup & smoke
| # | Request | Expected |
|---|---|---|
| 0.1–0.3 | Health of gateway / trip / location | 200 `UP` |
| 0.4 | Kafka REST cluster id | 200, `kafka_cluster_id` stored |
| 0.5–0.8 | Tokens for rider1, driver1, driver2, admin1 | 200; `*_token` and `*_id` (JWT `sub`) stored; role asserted in the JWT |

If 0.5 fails with `invalid_grant / Account is not fully set up`, the realm import didn't run: restart Keycloak (`docker compose restart keycloak`).

### Folder 1 – Edge security
| # | Proves | Expected |
|---|---|---|
| 1.1 / 1.2 | Missing / garbage token | 401 |
| 1.3 | Gateway RBAC: driver can't `POST /trips` | 403 |
| 1.4 | Gateway RBAC: rider can't update driver location | 403 |
| 1.5 | `/internal/**` denied at the edge even for ADMIN | 403 |
| 1.6 | **Zero trust**: calling trip-service directly without a JWT | 401 |
| 1.7 | Bean validation (lat 95°) | 400, nothing created |
| 1.8 | ABAC fail-closed on unknown trip | 403 (not 404) |

### Folder 2 – Location service (CQRS)
Flow: `PUT /drivers/me/location` → row in `driver_location` + `outbox` (one tx) → Debezium → Kafka `outbox.event.driver-location` → projector → Redis.

| # | Expected |
|---|---|
| 2.1 / 2.2 | 202 for driver1 (at pickup) and driver2 (~25 km away) |
| 2.3 | `nearby` (3 km) contains driver1, **not** driver2; sorted by distance |
| 2.4 | radius 50 km → 400 (cap is 10 km) |
| 2.5 / 2.6 | driver context 200 `AVAILABLE` / 404 for unknown driver |
| 2.7 | surge multiplier in [1, 3], `supply ≥ 1` |
| 2.8 / 2.9 | OFFLINE removes driver1 from search |
| 2.10 | back AVAILABLE (state needed by next folders) |
| 2.11 | longitude 200° → 400 |

Verify the write model and the read model:
```bash
cd infra/local
docker compose exec postgres psql -U postgres -d locationdb -c "select driver_id,lat,lng,status from driver_location;"
docker compose exec redis redis-cli GEOSEARCH geo:drivers:CAB FROMLONLAT 78.3772 17.4435 BYRADIUS 3 km ASC WITHDIST
docker compose exec redis redis-cli HGETALL driver:<driver1-sub>
```
Watch the raw CDC stream (note the `id` and `eventType` headers set by Debezium):
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic outbox.event.driver-location --from-beginning --property print.key=true --property print.headers=true
```

### Folder 3 – Happy path (the full saga)
| # | Request | Expected result | Saga state after |
|---|---|---|---|
| 3.1 | Rider books | 202, `REQUESTED`, fare > 0 | AWAITING_PAYMENT_HOLD |
| 3.2 | Second booking | 409 (one active trip per rider) | – |
| 3.3 | Rider reads trip | 200 `REQUESTED` | – |
| 3.4 | driver2 reads trip | 403 (ABAC VIEW) | – |
| 3.5 | admin reads trip | 200 | – |
| 3.6 | ⚙ PAYMENT_HELD | 200 from Kafka REST | AWAITING_DRIVER_MATCH |
| 3.7 | read trip | `PAYMENT_HELD` | – |
| 3.8 | ⚙ DRIVER_MATCHED (driver1) | 200 | AWAITING_DRIVER_ACCEPT |
| 3.9 | read trip | `DRIVER_OFFERED`, driverId = driver1 | – |
| 3.10 | driver2 accepts | 403 (offer belongs to driver1) | – |
| 3.11 | rider accepts | 403 (RBAC) | – |
| 3.12 | driver1 accepts | 200 `DRIVER_ASSIGNED` (ABAC: offered + AVAILABLE + ≤ 3 km) | DRIVER_ASSIGNED |
| 3.13 / 3.14 | driver moves 25 km away, tries to start | 403 (geofence 300 m) | – |
| 3.15 / 3.16 | driver returns, starts | 200 `IN_PROGRESS` | TRIP_IN_PROGRESS |
| 3.17 | rider cancels in progress | 403 | – |
| 3.18 | complete (8.5 km) | 200 `COMPLETED`, `finalFare = (50 + 14·8.5)·surge` | AWAITING_PAYMENT_CAPTURE |
| 3.19 | ⚙ PAYMENT_CAPTURED | 200 | COMPLETED |
| 3.20 / 3.21 | read / double-complete | `COMPLETED` / 403 | – |

Verify the transactional outbox and saga (run after the folder):
```bash
cd infra/local
docker compose exec postgres psql -U postgres -d tripdb -c \
 "select t.status,t.driver_id,t.final_fare,s.state from trips t join saga_instance s on s.trip_id=t.id order by t.created_at desc limit 3;"
docker compose exec postgres psql -U postgres -d tripdb -c \
 "select aggregatetype,type,created_at from outbox order by created_at desc limit 12;"
```
Expected outbox types (newest first): `CAPTURE_PAYMENT`, `TRIP_COMPLETED`, `TRIP_STARTED`, `DRIVER_ASSIGNED`, `DRIVER_OFFERED`, `MATCH_DRIVER`, `TRIP_REQUESTED`, `HOLD_PAYMENT`. (Rows older than 1 h are purged.)

Inspect the commands the orchestrator emitted (these are what a payment/matching service would consume):
```bash
for t in outbox.event.payment-command outbox.event.matching-command outbox.event.trip; do
  echo "=== $t"; docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic $t --from-beginning --timeout-ms 4000 --property print.key=true --property print.headers=true
done
```
Each record must carry headers `id:<uuid>,eventType:<TYPE>` and the trip id as key.

### Folder 4 – Compensation A (payment declined)
4.1 book → 4.2 ⚙ `PAYMENT_HOLD_FAILED` → 4.3 trip `CANCELLED`. Saga `CANCELLED`; outbox contains `TRIP_CANCELLED`; **no** `RELEASE_PAYMENT_HOLD` is emitted (nothing was held).

### Folder 5 – Compensation B (no driver)
5.3 ⚙ `DRIVER_MATCH_FAILED` → saga `COMPENSATING`, command **`RELEASE_PAYMENT_HOLD`** appears on `outbox.event.payment-command`. 5.4 proves the trip is still `PAYMENT_HELD` (cancel happens only after the release is confirmed). 5.5 ⚙ `PAYMENT_RELEASED` → 5.6 `CANCELLED` (local `CANCEL_BOOKING`).

### Folder 6 – Rider cancellation
6.2 cancel during payment-hold → **409** (saga refuses mid-step). After `PAYMENT_HELD`, 6.4 → 200 and the outbox gets `CANCEL_MATCH` + `RELEASE_PAYMENT_HOLD`; 6.5/6.6 finish with `CANCELLED`.

### Folder 7 – Idempotency & ordering
7.2 first delivery of event X; 7.3 **same** `id` replayed; 7.4 a **new** id but a stale reply.
```bash
docker compose exec postgres psql -U postgres -d tripdb -c "select count(*) from processed_event;"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic outbox.event.matching-command --from-beginning --timeout-ms 4000 --property print.key=true | grep <tripId>
```
Expected: exactly **one** `MATCH_DRIVER` for that trip; trip-service log contains `Duplicate event … ignored` (7.3) and `Ignoring stale/out-of-order PaymentHeld` (7.4).

### Folder 8 – Rate limiting
Runner → only request 8.1 → 30 iterations, delay 0. Route limit: **20 req / 10 s per user** (sliding window). Console shows `X-RateLimit-Remaining` 19 → 0, then **429**. Wait 10 s → allowed again.
```bash
docker compose exec redis redis-cli --scan --pattern 'rl:sw:*'       # the limiter's ZSETs
```

### Folder 9 – Resilience (manual)
Precondition: rider1 has no active trip (folders 3–7 finished).

1. **9.1** — stop `location-service` (Ctrl-C in its terminal), then send. Breakers `CLOSED`.
2. **9.2** book → 202 and `surgeMultiplier == 1` (**surge fallback**: pricing is down, booking isn't blocked).
3. **9.3 / 9.4** simulate hold + match (trip → `DRIVER_OFFERED`).
4. **9.5** accept → **403** because the driver's location can't be verified (**fail-closed ABAC**). Run it in the Runner **10 times** (each = up to 3 attempts with full-jitter backoff).
5. **9.6** `driverContext` breaker `OPEN`. Calls now fail instantly (no retries on `CallNotPermittedException`).
6. Restart `location-service`, **wait ≥ 30 s** (`waitDurationInOpenState`), send **9.7** (driver heartbeat), then **9.8** → 200 (HALF_OPEN probes succeed).
7. **9.9** breaker back to `CLOSED`; 9.10–9.12 clean up.

Breaker config to compare against (from `trip-service/application.yml`): window 50, min 20 calls, failure ≥ 40 % (driverContext), open 30 s, 5 half-open probes.

### Folder 10 – Gateway circuit breaker (manual)
Stop `trip-service`, send 10.1 → **503** with `Retry-After: 5` and an RFC 7807 body from the gateway fallback.

---

## 5. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Gateway returns 503 / `Unable to find instance for trip-service` | Eureka not running or the service hasn't registered yet (check http://localhost:8761; wait ~10 s after start). |
| Every call 401 from the services | `OIDC_ISSUER_URI` must equal the token's `iss` exactly (`http://localhost:8180/realms/ridehail`). Get tokens via `localhost:8180`, not `127.0.0.1` or a container name. |
| 2.3 returns `[]` | Connector not `RUNNING` (`curl localhost:8083/connectors/location-outbox-connector/status`), or raise `propagation_delay_ms`. |
| Connector fails with unknown `snapshot.mode` | Your Debezium is < 2.7: change `no_data` → `schema_only` in `infra/local/*.local.json` and re-register. |
| Connector fails: relation `public.outbox` missing | Services weren't started before registering. Start them, then re-run `register-local.sh`. |
| Simulate requests return 404 | Topics missing (`./create-topics.sh`) or empty `kafka_cluster_id` (run 0.4). |
| Simulate returns 200 but trip doesn't change | Not consumed: check trip-service log, group `trip-saga` lag: `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group trip-saga`. Also check for records in `outbox.event.payment.DLT`. |
| Simulate returns 4xx about `headers` | Older REST proxy without v3 headers. Produce with kcat instead: `echo '{"tripId":"<id>","holdId":"h1"}' \| kcat -b localhost:9092 -P -t outbox.event.payment -k <tripId> -H id=$(uuidgen) -H eventType=PAYMENT_HELD` |
| 3.12 accept → 403 unexpectedly | Driver not AVAILABLE or >3 km in Redis (re-run 2.10), trip not `DRIVER_OFFERED`, or `driverContext` breaker still OPEN. |
| Unexpected 429 | You exceeded 20 req/10 s (trip) or 10 req/10 s (driver location); wait 10 s. |
| 409 "active trip" when booking | A previous run left a trip active. Reset (below). |

### Reset between runs
```bash
cd infra/local
docker compose exec postgres psql -U postgres -d tripdb -c \
 "update trips set status='CANCELLED' where status in ('REQUESTED','PAYMENT_HELD','DRIVER_OFFERED','DRIVER_ASSIGNED','IN_PROGRESS'); update saga_instance set state='CANCELLED' where state not in ('COMPLETED','CANCELLED');"
docker compose exec redis redis-cli FLUSHALL        # drops the limiter keys and the driver read model
```
(After FLUSHALL re-send 2.1/2.10 so the drivers reappear in Redis.)

---

## 6. Caveats

* The project was **not compiled or executed** while this collection was written (no Maven Central access in the authoring environment). Treat a first failing assertion as a possible code or config bug, not necessarily a test bug; the Troubleshooting table lists the likeliest causes.
* Surge depends on live demand/supply, so assertions only check the range [1, 3], and the final-fare assertion multiplies by the returned multiplier.
* `infra/local/*` is for local testing only (superuser Debezium role, plain-text Kafka, Keycloak dev mode).


---
## 7. Mode C extras (matching-service + notification-service)

**Extra verification (from `infra/local`)**
```bash
# driver reservations / match outcomes
docker compose exec postgres psql -U postgres -d matchingdb -c "select trip_id,state,driver_id,distance_km,eta_seconds,searched_radius_km,failure_reason from match_request order by created_at desc limit 5;"
# notifications produced for each user
docker compose exec postgres psql -U postgres -d notificationdb -c "select user_id,type,title,read_at from notifications order by created_at desc limit 10;"
# simulated push/SMS deliveries appear in the notification-service log as [PUSH ] and [SMS  ] lines
```
**Speed up the offer-expiry test:** start matching-service with `OFFER_TIMEOUT_SECONDS=20` and lower the 70 s delay of request 12.20.
**Common Mode C problems:** trip stuck in `PAYMENT_HELD` ⇒ matching connector not registered or no driver ping in the last 30 s (re-run 12.1 right before booking); matching log shows `Connection refused` to location-service ⇒ location-service not registered in Eureka; Kafka retries it for ~45 s and then dead-letters to `outbox.event.matching-command.DLT`.
