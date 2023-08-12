FROM sbtscala/scala-sbt:eclipse-temurin-jammy-8u352-b08_1.9.3_2.12.18
WORKDIR /app
ADD . /app
RUN sbt test assembly

FROM eclipse-temurin:8u382-b05-jre
WORKDIR /app
COPY --from=0 /app/target/scala-2.12/top-songs-assembly-1.0.jar /app

ENTRYPOINT ["java", "-jar", "/app/top-songs-assembly-1.0.jar"]