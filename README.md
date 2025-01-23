# Kafka bits (in Scala)

In this repo you can find a set of example (in the form of tests) on how to integrate with Kafka 
in the following way:
* Kafka clients (lowest level of integration)
* Akka Streams (Used in consumption and production of messages)
* Kafka streams (Use for message transformation)

## Run tests

For simply running tests, run the following command
`sbt test`

### Requirements

* `docker`
* `sbt 1.5.1`
* `scala 2.13`

Hello!