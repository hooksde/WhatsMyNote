# WhatsMyNote — Piano MIDI → Kafka → Cloud DB

A learning project for hands-on Spring Boot microservices, event-driven
architecture, and Kafka on AWS. It captures notes from a physical MIDI piano
keyboard and streams two kinds of events — individual **notes** and derived
**chords** — through Kafka to a cloud database.

This file is the source of truth for the design. Read it before making changes.

---

## The one hard constraint

MIDI capture **must run on the machine the keyboard is physically plugged into**.
A cloud service cannot reach a USB device. So the capture component is always a
local "edge" producer; everything Kafka/event-driven lives in the cloud behind
it. That boundary is the backbone of the design — do not try to move capture
into a cloud service.

## Data flow

```
MIDI keyboard
  → MIDI capture app        (standalone JVM, on the local machine)
  → ingest service          (Spring Boot; POST /api/notes → produces to Kafka)
  → note-events topic        (Kafka / MSK)
  → chord detector          (Kafka Streams; consumes note-events)
  → chord-events topic       (Kafka / MSK)
  → sink service            (Spring Boot consumer; writes both topics)
  → Cloud DB                 (DynamoDB or Timestream)
```

`note-events` is consumed by both the chord detector and the sink. The chord
detector's output (`chord-events`) is also sinked. Both topics reach the DB.

## Module layout

| Path              | What it is                        | Runtime                    |
|-------------------|-----------------------------------|----------------------------|
| `midi-capture/`   | Standalone edge producer          | Plain Java (no Spring)     |
| `chord-detection/`| Kafka Streams chord detector      | Spring Boot                |
| `sink-service/`   | Kafka consumer → cloud DB         | Spring Boot                |
| `local-dev/`      | docker-compose (Kafka + DynamoDB) | Docker                     |
| `infra/`          | Terraform for the AWS side        | Terraform / AWS            |

The **ingest service** is described but not yet implemented — see "Current
status" below.

## Key design decisions (and why)

1. **Notes are raw; chords are derived.** MIDI has no chord concept — the
   keyboard only emits NoteOn/NoteOff. A chord is an inference over the note
   stream. So note events are 1:1 with MIDI messages (trivial), and chord events
   come from stream processing. This split is the whole reason Kafka Streams is
   in the project — treat it as the core learning goal, not a queue.

2. **Two interchangeable chord-detection strategies**, selected by Spring profile:
   - `session` → `ChordDetectionTopology`: DSL session windows. Groups notes
     struck within ~50ms. Detects "struck together" (block chords, strums).
   - `overlap` → `ChordProcessorTopology` + `ChordProcessor`: Processor API +
     a `KeyValueStore` tracking held notes. Detects true "held together"
     overlap, including sustained/pedal chords.
   Keep exactly one active. Both are guarded by `@Profile`; default is `session`.

3. **The edge talks to an ingest gateway, not to Kafka directly.** Better
   security (no broker creds on the edge), decoupling, and it mirrors real
   IoT/device patterns. The capture app just POSTs JSON.

4. **Partition by `sourceId`** (the keyboard id). Keeps per-keyboard ordering
   and lets you reconstruct a performance. The ingest producer uses `sourceId`
   as the Kafka message key.

5. **NoteOff is often a NoteOn with velocity 0** (running status). The capture
   app treats both as a release. Any note-parsing code must handle both.

6. **The capture app sends async, fire-and-forget** so the MIDI callback thread
   never blocks on the network — otherwise playing feels laggy.

7. **`suppress(untilWindowCloses)`** in the session topology so each chord emits
   once, not on every keypress.

8. **Sinks sit behind an `EventSink` interface**, one impl per DB
   (`dynamodb` default / `timestream`), selected by profile. No credentials in
   code — the AWS SDK uses the standard chain (env/profile/role).

9. **MSK Serverless with IAM auth** — no passwords. The Fargate task role grants
   `kafka-cluster:*` and DynamoDB writes. See the MSK glue note below.

## Gotchas to respect

- **Session windows fire on event time**, so the *last* chord of a burst can sit
  unflushed until the next note arrives. For interactive use, add a wall-clock
  punctuator or a stream-time heartbeat. (TODO — not implemented.)
- **The overlap detector only shrinks the held set on NoteOff.** A dropped
  NoteOff leaves a "stuck" note. Add a periodic punctuator that evicts notes
  held implausibly long. (TODO — not implemented.)
- **Cloud round-trip is not low-latency** (tens–hundreds of ms). Fine for
  capture-and-store; do not build live audio feedback through this loop.
- **The event contracts (`NoteEvent`, `ChordEvent`) are duplicated** across
  modules on purpose (each is a separate deployable). In a real system, share
  them via a schema/registry or a common module. Keep the shapes in sync.

## Tech stack

- Java 17+ (records, pattern matching, text blocks)
- Spring Boot 3.x, `spring-kafka` (incl. Kafka Streams integration)
- Apache Kafka / Kafka Streams; Amazon MSK Serverless in the cloud
- AWS SDK v2 (`dynamodb`, `timestreamwrite`)
- Terraform (`~> 5.0` AWS provider), ECS Fargate, ECR, DynamoDB
- Local: Docker (Confluent cp-kafka in KRaft mode, DynamoDB Local)

## Build & run (local)

```bash
# 1. Infra: Kafka + UI + DynamoDB Local, topics + tables auto-created
cd local-dev && docker compose up -d      # Kafka UI at http://localhost:8085

# 2. Sink needs to reach DynamoDB Local (SDK reads these from the environment):
export AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000
export AWS_ACCESS_KEY_ID=local
export AWS_SECRET_ACCESS_KEY=local
export AWS_REGION=us-east-1

# 3. Run the services (each its own Spring Boot app):
#    - ingest            (port 8080)
#    - chord-detection   SPRING_PROFILES_ACTIVE=session|overlap
#    - sink              SPRING_PROFILES_ACTIVE=dynamodb
# 4. Run the standalone capture app and play:
#    java com.job4me.midi.MidiCaptureApp ["<device-name-substring>"]
```

The apps default to `localhost:9092`, so no Kafka config is needed locally.

## Deploy (AWS)

```bash
cd infra && terraform init && terraform apply
```

`terraform apply` creates empty ECR repos, so the services won't start until you
build/tag/push images (see the `ecr_repos` output), then let ECS pull them.
Set the capture app's `INGEST_URL` to the `ingest_url` output. Run
`terraform destroy` when idle — MSK Serverless and NAT bill hourly.

### MSK IAM glue (required)

Terraform grants the permissions, but each Spring app must also use the auth
library to exercise them. Add the `software.amazon.msk:aws-msk-iam-auth`
dependency and these properties (bootstrap endpoint is injected as
`SPRING_KAFKA_BOOTSTRAP_SERVERS`):

```properties
spring.kafka.properties.security.protocol=SASL_SSL
spring.kafka.properties.sasl.mechanism=AWS_MSK_IAM
spring.kafka.properties.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
spring.kafka.properties.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

## Conventions

- Packages: `com.job4me.midi`, `com.job4me.chord`, `com.job4me.sink`.
- One responsibility per module; modules are independently deployable.
- Keep `NoteEvent`/`ChordEvent` field names identical across modules.
- New AWS permissions go on the **task role** (`infra/iam.tf`), not the
  execution role.

## Current status

**Implemented:** MIDI capture app; both chord-detection topologies; sink service
(DynamoDB + Timestream); local docker-compose; Terraform for MSK/DynamoDB/ECR/
ECS/Fargate/ALB.

**Not yet built (good next tasks):**
- Build files: a Maven/Gradle setup — parent + one module per service.
- `@SpringBootApplication` main classes for `chord-detection` and `sink-service`.
- The **ingest service**: a Spring Boot app with a `POST /api/notes` controller
  that produces to `note-events` keyed by `sourceId`, plus its own module.
- `spring-boot-starter-actuator` on ingest (the ALB health check hits
  `/actuator/health`).
- The MSK IAM properties above, wired into each app's cloud config/profile.
- Wall-clock punctuators: last-chord flush (session) and stuck-note eviction
  (overlap).
- Tests, and optionally a read-side web dashboard over the DB.
