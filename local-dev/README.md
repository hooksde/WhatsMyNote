# Local dev stack

Runs the whole pipeline on your machine before touching AWS: Kafka (KRaft),
a Kafka web UI, and DynamoDB Local — with topics and tables pre-created.

## Start

```bash
docker compose up -d
```

- Kafka broker: `localhost:9092`
- Kafka UI: http://localhost:8085
- DynamoDB Local: `localhost:8000`

The `kafka-init` and `dynamodb-init` containers create `note-events` /
`chord-events` topics and tables, then exit. That's expected.

## Point the apps at local infra

All three Spring apps already default to `localhost:9092`, so they need no
changes for Kafka. The sink app additionally needs DynamoDB Local — the AWS SDK
reads an endpoint override straight from the environment, so no code change:

```bash
export AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000
export AWS_ACCESS_KEY_ID=local
export AWS_SECRET_ACCESS_KEY=local
export AWS_REGION=us-east-1
```

## Run order

1. `docker compose up -d`
2. Start the **ingest** service (Spring Boot, port 8080)
3. Start the **chord-detection** app — pick a detector with the profile:
   `SPRING_PROFILES_ACTIVE=session` or `=overlap`
4. Start the **sink** service with the env vars above (`dynamodb` profile)
5. Run the standalone **MIDI capture** app and play

Watch events land in the Kafka UI, then verify storage:

```bash
aws dynamodb scan --endpoint-url http://localhost:8000 --table-name chord-events
```

## Stop

```bash
docker compose down
```
