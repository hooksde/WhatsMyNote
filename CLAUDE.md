# WhatsMyNote — Piano MIDI → Kafka → Cloud DB

A learning project for hands-on Spring Boot microservices, event-driven
architecture, and Kafka in the cloud. It captures notes from a physical MIDI
piano keyboard and streams two kinds of events — individual **notes** and
derived **chords** — through Kafka to a cloud database. Deployable to either
AWS or Azure from the same application code; only the Spring profile and the
Terraform stack differ.

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
  → note-events topic        (Kafka / MSK / Event Hubs)
  → chord detector          (Kafka Streams; consumes note-events)
  → chord-events topic       (Kafka / MSK / Event Hubs)
  → sink service            (Spring Boot consumer; writes both topics)
  → Cloud DB                 (DynamoDB, Timestream, or Cosmos DB)
```

`note-events` is consumed by both the chord detector and the sink. The chord
detector's output (`chord-events`) is also sinked. Both topics reach the DB.

## Module layout

| Path                | What it is                             | Runtime                 |
|---------------------|-----------------------------------------|--------------------------|
| `midi-capture/`     | Standalone desktop edge producer        | Plain Java (no Spring)   |
| `android-capture/`  | Android edge producer (USB MIDI host)   | Kotlin / Gradle          |
| `ingest/`           | HTTP → Kafka gateway                    | Spring Boot              |
| `chord-detection/`  | Kafka Streams chord detector            | Spring Boot              |
| `sink-service/`     | Kafka consumer → cloud DB               | Spring Boot              |
| `local-dev/`        | docker-compose (Kafka + DynamoDB Local) | Docker                   |
| `infra/`            | Terraform for the AWS deployment        | Terraform / AWS          |
| `infra-azure/`      | Terraform for the Azure deployment      | Terraform / Azure        |

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
   (`dynamodb` default / `timestream` on AWS, `cosmosdb` on Azure), selected by
   profile. No credentials in code — the AWS SDK uses its standard credential
   chain (env/profile/role); the Cosmos sink uses `DefaultAzureCredential`
   (the container's managed identity).

9. **No passwords for either cloud's Kafka, mostly.** MSK Serverless uses IAM
   auth (`kafka-cluster:*` on the Fargate task role, zero secrets). Event Hubs'
   Kafka head only supports SASL/PLAIN with a connection string, so that one
   secret lives in Key Vault and is referenced by the container's managed
   identity rather than injected as a plain env var. See the auth glue notes
   below for both.

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
- Apache Kafka / Kafka Streams
- **AWS**: MSK Serverless (Kafka), AWS SDK v2 (`dynamodb`, `timestreamwrite`),
  Terraform (`~> 5.0` `aws` provider), ECS Fargate, ECR, ALB, IAM task role
- **Azure**: Event Hubs Standard (Kafka-compatible endpoint), Azure SDK
  (`azure-cosmos`, `azure-identity`), Terraform (`~> 4.0` `azurerm` provider),
  Container Apps, ACR, Key Vault, user-assigned managed identity
- Local: Docker (Confluent cp-kafka in KRaft mode, DynamoDB Local) — same
  stack regardless of which cloud you deploy to; no cloud profile is active
  locally

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

## Android edge client (android-capture/)

A second edge producer -- same role as `midi-capture/`, same wire contract
(`NoteEvent` JSON, `POST <ingest-url>/api/notes`), but running on a phone with
a USB MIDI keyboard plugged in via OTG instead of a desktop with an onboard
MIDI port. Separate Gradle/Kotlin build, not part of the Maven reactor, same
as `midi-capture/` isn't either.

- `MidiNoteReceiver` parses the raw USB MIDI byte stream (`android.media.midi`)
  the same way the desktop app's `NoteReceiver` parses `ShortMessage`s --
  including the same NOTE_ON-with-velocity-0-means-release handling.
- `HttpEventSender` POSTs fire-and-forget on a background executor, same
  reasoning as the desktop app: never block the MIDI callback on the network.
- `minSdk 33` (uses `MidiManager.getDevicesForTransport`, which replaced the
  deprecated `getDevices()` in API 33) -- fine since this targets a Fold 5,
  not old hardware.
- The manifest declares `android.hardware.usb.host` and `android.software.midi`
  as required features.

```bash
cd android-capture
./gradlew assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
adb logcat | grep MidiCapture   # while it's running, to watch it work
```

In the app, set the Ingest URL to wherever `ingest` is actually reachable from
the phone -- **not `localhost`**, that would mean the phone itself. On the
same Wi-Fi as a locally-running `ingest`, use the host machine's LAN IP
(`http://<lan-ip>:8080/api/notes`); against a cloud deployment, use the
`ingest_url` Terraform output.

## Container images

Each of `ingest`/`chord-detection`/`sink-service` has its own `Dockerfile`, but
the **build context is the repo root**, not the module directory — the reactor
needs every module's `pom.xml` to resolve, even though `-pl <module> -am` only
compiles that module (there are no dependency edges between the three, so
`-am` doesn't pull in the others' source). `midi-capture` has no Dockerfile;
it's a plain Java app that runs on the machine with the keyboard, never in a
container.

```bash
docker build -f ingest/Dockerfile          -t ingest:latest          .
docker build -f chord-detection/Dockerfile -t chord-detection:latest .
docker build -f sink-service/Dockerfile    -t sink-service:latest    .
```

Tag and push each to the registry your cloud's Terraform output gives you (see
the AWS/Azure sections below) before the deployed apps can start.

## Deploy (AWS)

```bash
cd infra && terraform init && terraform apply
```

`terraform apply` creates empty ECR repos, so the services won't start until you
build (see "Container images" above), tag, and push images to the `ecr_repos`
output, then let ECS pull them:

```bash
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker tag ingest:latest <ecr_repos.ingest>:latest && docker push <ecr_repos.ingest>:latest
# ...repeat for chord-detection and sink
```

Set the capture app's `INGEST_URL` to the `ingest_url` output. Run
`terraform destroy` when idle — MSK Serverless and NAT bill hourly.

### MSK IAM glue

Terraform grants the permissions, but each Spring app must also use the auth
library to exercise them. Each of `ingest`/`chord-detection`/`sink-service`
depends on `software.amazon.msk:aws-msk-iam-auth`, and their `application-cloud.yml`
sets (bootstrap endpoint is injected as `SPRING_KAFKA_BOOTSTRAP_SERVERS`):

```properties
spring.kafka.properties.security.protocol=SASL_SSL
spring.kafka.properties.sasl.mechanism=AWS_MSK_IAM
spring.kafka.properties.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
spring.kafka.properties.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Activate with `SPRING_PROFILES_ACTIVE=<topology-or-sink-profile>,cloud` (Terraform
already sets this per service in `infra/variables.tf`'s `local.services`).

## Deploy (Azure)

```bash
cd infra-azure && terraform init && terraform apply
```

`terraform apply` creates an empty ACR, so the apps won't start until you build
(see "Container images" above), tag, and push images to the `acr_login_server`
output, then let Container Apps pull them:

```bash
az acr login --name <acr-name>
docker tag ingest:latest <acr_login_server>/ingest:latest && docker push <acr_login_server>/ingest:latest
# ...repeat for chord-detection and sink
```

Set the capture app's `INGEST_URL` to the `ingest_url` output. Run
`terraform destroy` when idle — Event Hubs Standard and Cosmos DB bill regardless
of traffic (Cosmos is serverless/pay-per-request, but the Event Hubs namespace
is an hourly charge).

### Event Hubs auth glue

Event Hubs' Kafka-compatible endpoint only supports SASL/PLAIN with a
connection string — there's no IAM-style passwordless option here the way MSK
has. Each app's `application-azure.yml` sets (bootstrap endpoint is injected as
`SPRING_KAFKA_BOOTSTRAP_SERVERS`; `$ConnectionString` below is a literal Event
Hubs requires, not a placeholder):

```properties
spring.kafka.properties.security.protocol=SASL_SSL
spring.kafka.properties.sasl.mechanism=PLAIN
spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="${EVENTHUB_CONNECTION_STRING}";
```

The connection string itself lives in Key Vault (`infra-azure/keyvault.tf`) and
is wired to each Container App as a Key-Vault-backed secret (`infra-azure/containerapps.tf`),
not a plaintext env var. Cosmos DB, by contrast, *is* passwordless — auth is
via the container's user-assigned managed identity (`infra-azure/identity.tf`
grants it the Cosmos DB Built-in Data Contributor role), consumed through
`DefaultAzureCredential` in `CosmosDbEventSink`.

Activate with `SPRING_PROFILES_ACTIVE=<topology-or-sink-profile>,azure`
(Terraform already sets this per service in `infra-azure/variables.tf`'s
`local.services`).

## Conventions

- Packages: `com.job4me.midi`, `com.job4me.chord`, `com.job4me.sink`,
  `com.job4me.ingest`, `com.job4me.midicapture` (Android).
- One responsibility per module; modules are independently deployable.
- Keep `NoteEvent`/`ChordEvent` field names identical across modules.
- New AWS permissions go on the **task role** (`infra/iam.tf`), not the
  execution role. New Azure permissions go on the shared **app identity**
  (`infra-azure/identity.tf`).
- Profile combinations (topology/sink profile + cloud profile, comma-separated
  in `SPRING_PROFILES_ACTIVE`):

  | Module            | Local (default)      | AWS                        | Azure                       |
  |-------------------|-----------------------|-----------------------------|------------------------------|
  | `ingest`          | *(none)*              | `cloud`                     | `azure`                      |
  | `chord-detection` | `session` / `overlap` | `session,cloud` (or overlap)| `session,azure` (or overlap) |
  | `sink-service`    | `dynamodb`            | `dynamodb,cloud` (or timestream) | `cosmosdb,azure`        |

## Current status

**Implemented:** Desktop MIDI capture app; Android MIDI capture app
(`android-capture/`, USB host mode, builds clean with `./gradlew
assembleDebug`); ingest service (`POST /api/notes`, actuator health check);
both chord-detection topologies, each with a wall-clock punctuator
(session-window last-chord flush via heartbeat injection; overlap stuck-note
eviction); sink service (DynamoDB, Timestream, and Cosmos DB); Maven
multi-module build; per-module Dockerfiles; local docker-compose; Terraform
for both AWS (MSK/DynamoDB/ECR/ECS/Fargate/ALB) and Azure (Event
Hubs/Cosmos DB/ACR/Container Apps/Key Vault); MSK IAM and Event Hubs SASL auth
wiring; unit + topology tests for chord-detection, sink-service, and ingest.

**Not yet built (good next tasks):**
- The Android app has never run on a physical device -- built and verified
  via `aapt2 dump badging` only. Needs an on-device pass: install, open a USB
  MIDI keyboard, confirm notes actually reach `ingest`.
- A read-side web dashboard over either DB.
- `infra-azure` uses one Event Hubs connection string (namespace-level SAS) for
  all three apps; scoping a separate SAS rule per app (or moving to Azure AD
  OAUTHBEARER auth for Event Hubs) would tighten this.
- Neither Terraform stack has been run through `terraform validate`/`plan`
  against real cloud credentials in this environment — review before applying.
- Whether MSK Serverless auto-creates topics on first produce is unconfirmed;
  if it doesn't, `note-events`/`chord-events` need to be created explicitly via
  an IAM-authenticated Kafka admin client before traffic will flow.
- No CI/CD — deploying either cloud today is manual `terraform apply` +
  manual `docker build`/tag/push.
