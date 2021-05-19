import Dependencies.{testContainers, testContainersKafka, _}

ThisBuild / scalaVersion     := "2.13.5"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "org.aalexandre"
ThisBuild / organizationName := "aalexandre"

name             := "kafka-bits"
libraryDependencies ++= Seq(
  slf4j,
  logback,
  scalaLogging,

  kafkaClients,
  kafkaStreams,

  akkaSlf4j,
  akkaStream,
  akkaStreamKafka,
  jacksonDatabind,

  scalaTest % Test,
  testContainers % Test,
  testContainersKafka % Test
)