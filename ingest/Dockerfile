# Build context is the repo root (docker build -f ingest/Dockerfile .) -- the
# reactor needs every module's pom.xml to resolve, even though -am only
# actually compiles the ones ingest depends on (none, here).
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .
COPY ingest/pom.xml ingest/
COPY chord-detection/pom.xml chord-detection/
COPY sink-service/pom.xml sink-service/
RUN ./mvnw -q -pl ingest -am dependency:go-offline

COPY ingest/src ingest/src
RUN ./mvnw -q -pl ingest -am package -DskipTests

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app
COPY --from=build /workspace/ingest/target/ingest-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
