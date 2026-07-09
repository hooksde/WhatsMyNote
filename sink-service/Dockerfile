# Build context is the repo root (docker build -f sink-service/Dockerfile .).
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .
COPY ingest/pom.xml ingest/
COPY chord-detection/pom.xml chord-detection/
COPY sink-service/pom.xml sink-service/
RUN ./mvnw -q -pl sink-service -am dependency:go-offline

COPY sink-service/src sink-service/src
RUN ./mvnw -q -pl sink-service -am package -DskipTests

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app
COPY --from=build /workspace/sink-service/target/sink-service-*.jar app.jar
# No exposed port -- this is a Kafka consumer, not a web service.
ENTRYPOINT ["java", "-jar", "app.jar"]
