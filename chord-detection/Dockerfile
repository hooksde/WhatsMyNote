# Build context is the repo root (docker build -f chord-detection/Dockerfile .).
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .
COPY ingest/pom.xml ingest/
COPY chord-detection/pom.xml chord-detection/
COPY sink-service/pom.xml sink-service/
RUN ./mvnw -q -pl chord-detection -am dependency:go-offline

COPY chord-detection/src chord-detection/src
RUN ./mvnw -q -pl chord-detection -am package -DskipTests

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app
COPY --from=build /workspace/chord-detection/target/chord-detection-*.jar app.jar
# No exposed port -- this is a Kafka Streams consumer, not a web service.
ENTRYPOINT ["java", "-jar", "app.jar"]
