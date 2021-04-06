FROM maven:3-openjdk-11-slim AS build
COPY src /usr/src/app/src
COPY pom.xml /usr/src/app
RUN mvn -f /usr/src/app/pom.xml clean package -DskipTests

FROM adoptopenjdk/openjdk11:alpine-jre
COPY --from=build /usr/src/app/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/app/replicator.jar
ENTRYPOINT ["java","-Xms128m","-Xmx128m","-jar","/usr/app/replicator.jar"]