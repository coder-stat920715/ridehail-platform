# Kafka topic contract

Topics are created by IaC (Terraform / MSK), not by the apps. Every topic needs a matching `<topic>.DLT`
with the same partition count (dead-letter publishing keeps the original partition).

| Topic | Produced by (outbox aggregatetype) | Key | Consumers | Partitions (start) |
|---|---|---|---|---|
| outbox.event.trip | trip-service (`trip`) | tripId | notification, analytics, matching | 24 |
| outbox.event.payment-command | trip-service saga (`payment-command`) | tripId | payment-service | 24 |
| outbox.event.matching-command | trip-service saga (`matching-command`) | tripId | matching-service | 24 |
| outbox.event.payment | payment-service (`payment`) | tripId | trip-service saga | 24 |
| outbox.event.matching | matching-service (`matching`) | tripId | trip-service saga | 24 |

Also consumed: `outbox.event.trip` by **matching-service** (DRIVER_ASSIGNED / TRIP_COMPLETED / TRIP_CANCELLED → reservation lifecycle) and **notification-service** (all lifecycle events → inbox/push/SMS).
| outbox.event.driver-location | location-service (`driver-location`) | driverId | location query projector, matching | 48 |

Headers added by Debezium EventRouter: `id` (outbox row UUID -> idempotency key), `eventType`.

## Participant contract (payment-service, matching-service and notification-service are all implemented in this repo)

| Command (topic) | Reply event types (topic) | Reply payload |
|---|---|---|
| HOLD_PAYMENT (payment-command) | PAYMENT_HELD / PAYMENT_HOLD_FAILED (payment) | `{tripId, holdId}` / `{tripId, reason}` |
| RELEASE_PAYMENT_HOLD (payment-command) | PAYMENT_RELEASED (payment) | `{tripId}` |
| CAPTURE_PAYMENT (payment-command) | PAYMENT_CAPTURED / PAYMENT_CAPTURE_FAILED (payment) | `{tripId}` / `{tripId, reason}` |
| MATCH_DRIVER (matching-command) | DRIVER_MATCHED / DRIVER_MATCH_FAILED (matching) | `{tripId, driverId, etaSeconds}` / `{tripId, reason}` |
| CANCEL_MATCH (matching-command) | none | - |

Participants must: handle commands idempotently (dedupe on header `id`), and write the reply to THEIR
OWN outbox in the same transaction as their state change.
