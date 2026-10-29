# One Dockerfile for ALL services:  docker build --build-arg MODULE=trip-service -t trip-service .
# Stage 1 compiles only the requested module (+ the shared libs it depends on via -am).
FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -pl ${MODULE} -am -DskipTests package

# Stage 2: small JRE-only runtime image running as a NON-ROOT user.
FROM eclipse-temurin:21-jre-alpine
ARG MODULE
RUN addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-*.jar /app/app.jar
USER 10001
# Size the heap from the container limit (not the node's RAM); die on OOM so Kubernetes restarts the pod.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
