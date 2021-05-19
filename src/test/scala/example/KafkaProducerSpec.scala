package example

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}
import org.scalatest.matchers._

import java.util.{Collections, Properties}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}
import scala.util.Using

class KafkaProducerSpec extends KakfaSpec with should.Matchers with StrictLogging {

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global

  val intSerializer: Serializer[Int] = (_: String, data: Int) => BigInt(data).toByteArray
  val intDeserializer: Deserializer[Int] = (_: String, data: Array[Byte]) => BigInt(data).toInt

  "Kafka clients" should "publish 5 messages and consumes same 5" in {

    // admin
    val topic = new NewTopic("hello_world_topic", 8, 3.toShort)
    val adminSettings = {
      val properties = new Properties()
      properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
      properties
    }

    Using(Admin.create(adminSettings)) { admin =>
      admin.createTopics(Collections.singleton(topic))
    }

    // producer
    val producerSettings = new Properties()
    producerSettings.put(ProducerConfig.CLIENT_ID_CONFIG, "basic-producer-v0.1.0")
    producerSettings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    producerSettings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerSettings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producedSet = Using(new KafkaProducer[String, String](producerSettings)) { producer =>
      (1 to 5).map { idx =>
        val key = s"key-$idx"
        val value = s"value-$idx"

        val record = new ProducerRecord[String, String](topic.name(), key, value)
        (producer.send(record), record)
      }
    }.get
      .map(tuple => Future {
        (tuple._1.get, tuple._2)
      })
      .map(Await.result(_, 10.seconds)).toSet

    // consumer
    val consumerSettings = new Properties()
    consumerSettings.put(ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer-v0.1.1")
    consumerSettings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    consumerSettings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerSettings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerSettings.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000)
    consumerSettings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumedSet = Using(new KafkaConsumer[String, String](consumerSettings)) { consumer =>

      consumer.subscribe(Collections.singleton(topic.name()))

      var seq = Seq.empty[ConsumerRecord[String, String]]
      var records: ConsumerRecords[String, String] = ConsumerRecords.empty()

      do {
        records = consumer.poll(java.time.Duration.ofSeconds(5))
        seq ++= records.asScala.toSeq
      } while (!records.isEmpty)

      seq
    }.get.toSet

    assert(producedSet.size == consumedSet.size)
    assert {
      producedSet.map { t => (t._2.key(), t._2.value()) } == consumedSet.map { t => (t.key(), t.value()) }
    }

  }

  "Akka streams" should "stream 5 messages into kafka and consume it as a stream" in {

    implicit val actorSystem: ActorSystem = ActorSystem("test-system")
    implicit val executionContext: ExecutionContext = ExecutionContext.global

    // admin
    val topic = new NewTopic("hello_world_topic", 8, 3.toShort)
    val adminSettings = new Properties()
    adminSettings.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)

    Using(Admin.create(adminSettings)) { admin =>
      admin.createTopics(Collections.singleton(topic))
    }

    // akka stream producer
    val producerSettings =
      ProducerSettings.create(actorSystem, new StringSerializer, new StringSerializer)
        .withProperty(ProducerConfig.CLIENT_ID_CONFIG, "basic-producer-v0.1.0")
        .withBootstrapServers(kafka.getBootstrapServers)

    val future: Future[Seq[(RecordMetadata, ProducerRecord[String, String])]] =
      Source.fromIterator(() => (1 to 5).iterator)
        .map { idx =>
          val key = s"key-$idx"
          val value = s"value-$idx"
          val record = new ProducerRecord[String, String](topic.name(), key, value)
          ProducerMessage.Message(record, ())
        }
        .via(Producer.flexiFlow(producerSettings))
        .map {
          case ProducerMessage.Result(metadata, message) =>
            Seq((metadata, message.record))
          case ProducerMessage.MultiResult(parts, _) =>
            parts.map(e => (e.metadata, e.record))
          case ProducerMessage.PassThroughResult(_) =>
            Seq.empty
        }
        .runWith(Sink.seq).map(e => e.flatten)

    val producedSet = Await.result(future, 10.seconds).toSet

    // akka stream consumer
    val consumerSettings: ConsumerSettings[String, String] =
      ConsumerSettings.create(actorSystem, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(kafka.getBootstrapServers)
        .withGroupId("akka-stream-v0.1.0")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withStopTimeout(5.seconds)

    val sourceFuture: Future[Seq[ConsumerRecord[String, String]]] =
      Consumer.plainSource(consumerSettings, Subscriptions.topics(topic.name()))
        .log("consumed-event")
        .take(producedSet.size)
        .runWith(Sink.seq)
    val consumedSet = Await.result(sourceFuture, 30.seconds).toSet

    assert(producedSet.size == consumedSet.size)
    assert(producedSet.map(e => (e._2.key(), e._2.value())) == consumedSet.map(e => (e.key(), e.value())))
  }

  "Akka streams" should "stream 5 messages throught 2 kafka topics with transform between" in {

    implicit val actorSystem: ActorSystem = ActorSystem("test-system")
    implicit val executionContext: ExecutionContext = ExecutionContext.global

    // admin
    val firstTopic = new NewTopic("hello_world_first_topic", 8, 3.toShort)
    val secondTopic = new NewTopic("hello_world_second_topic", 8, 3.toShort)
    val adminSettings = new Properties()
    adminSettings.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)

    Using(Admin.create(adminSettings)) { admin =>
      admin.createTopics(List(firstTopic, secondTopic).asJava)
    }

    // akka stream producer Seq => Kafka
    val sourceFuture =
      Source.fromIterator(() => (1 to 5).iterator)
        .map { idx =>
          val record = new ProducerRecord[Int, Int](firstTopic.name(), idx, idx)
          ProducerMessage.Message(record, ())
        }
        .via(Producer.flexiFlow(
          ProducerSettings.create(actorSystem, intSerializer, intSerializer)
            .withProperty(ProducerConfig.CLIENT_ID_CONFIG, "basic-producer-v0.1.0")
            .withBootstrapServers(kafka.getBootstrapServers)
        ))
        .runWith(Sink.seq)

    val producedSet = Await.result(sourceFuture, 10.seconds)
      .flatMap {
        case ProducerMessage.Result(metadata, message) => Seq((metadata, message.record))
        case ProducerMessage.MultiResult(parts, _) => parts.map(e => (e.metadata, e.record))
        case ProducerMessage.PassThroughResult(_) => Seq.empty
      }
      .map(e => (e._2.key(), e._2.value()))
      .toSet

    // akka stream from Kafka => Kafka (with transformation)
    val transformFuture = Consumer.plainSource(
      ConsumerSettings.create(actorSystem, intDeserializer, intDeserializer)
        .withBootstrapServers(kafka.getBootstrapServers)
        .withGroupId("akka-stream-v0.1.0")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withStopTimeout(5.seconds),
      Subscriptions.topics(firstTopic.name())
    ).log("transform-event")
      .map(e => new ProducerRecord[String, String](secondTopic.name(), s"key-${e.key()}", s"value-${e.value()}"))
      .map(e => ProducerMessage.Message(e, ()))
      .via(Producer.flexiFlow(
        ProducerSettings.create(actorSystem, new StringSerializer, new StringSerializer)
          .withProperty(ProducerConfig.CLIENT_ID_CONFIG, "basic-producer-v0.1.0")
          .withBootstrapServers(kafka.getBootstrapServers)
      ))
      .take(producedSet.size)
      .runWith(Sink.seq)

    val transformSet = Await.result(transformFuture, 30.seconds)
      .flatMap {
        case ProducerMessage.Result(metadata, message) => Seq((metadata, message.record))
        case ProducerMessage.MultiResult(parts, _) => parts.map(e => (e.metadata, e.record))
        case ProducerMessage.PassThroughResult(_) => Seq.empty
      }.map(e => (e._2.key(), e._2.value())).toSet

    // akka stream consumer from Kafka to Seq
    val consumerSettings: ConsumerSettings[String, String] =
      ConsumerSettings.create(actorSystem, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(kafka.getBootstrapServers)
        .withGroupId("akka-stream-v0.1.0")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withStopTimeout(5.seconds)

    val sinkFuture =
      Consumer.plainSource(consumerSettings, Subscriptions.topics(secondTopic.name()))
        .log("consumed-event")
        .take(producedSet.size)
        .runWith(Sink.seq)
    val consumedSet = Await.result(sinkFuture, 30.seconds)
      .map(e => (e.key(), e.value()))
      .toSet

    assert(producedSet.size == transformSet.size)
    assert(transformSet.size == consumedSet.size)
    assert(transformSet == consumedSet)
  }

  "Kafka streams" should "publish 5 messages, transform and consumes same 5" in {

    implicit val actorSystem: ActorSystem = ActorSystem("test-system")
    implicit val executionContext: ExecutionContext = ExecutionContext.global

    // admin
    val firstTopic = new NewTopic("hello_world_first_topic", 8, 3.toShort)
    val secondTopic = new NewTopic("hello_world_second_topic", 8, 3.toShort)
    val adminSettings = new Properties()
    adminSettings.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)

    Using(Admin.create(adminSettings)) { admin =>
      admin.createTopics(List(firstTopic, secondTopic).asJava)
    }

    // akka stream producer Seq => Kafka
    val sourceFuture =
      Source.fromIterator(() => (1 to 5).iterator)
        .map { idx =>
          val record = new ProducerRecord[Int, Int](firstTopic.name(), idx, idx)
          ProducerMessage.Message(record, ())
        }
        .via(Producer.flexiFlow(
          ProducerSettings.create(actorSystem, intSerializer, intSerializer)
            .withProperty(ProducerConfig.CLIENT_ID_CONFIG, "basic-producer-v0.1.0")
            .withBootstrapServers(kafka.getBootstrapServers)
        ))
        .runWith(Sink.seq)

    val producedSet = Await.result(sourceFuture, 10.seconds)
      .flatMap {
        case ProducerMessage.Result(metadata, message) => Seq((metadata, message.record))
        case ProducerMessage.MultiResult(parts, _) => parts.map(e => (e.metadata, e.record))
        case ProducerMessage.PassThroughResult(_) => Seq.empty
      }
      .map(e => (e._2.key(), e._2.value()))
      .toSet

    // kafka streams (with transformation)
    val kafkaStreamsSettings = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-v0.1.0")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()

    builder.stream[Int, Int](firstTopic.name(), Consumed.`with`(Serdes.intSerde, Serdes.intSerde))
      .map((k, v) => new KeyValue(s"key-$k", s"value-$v"))
      .to(secondTopic.name())

    Using(new KafkaStreams(builder.build(), kafkaStreamsSettings)) { streams =>
      streams.start()
    }

    // akka stream consumer from Kafka to Seq
    val consumerSettings: ConsumerSettings[String, String] =
      ConsumerSettings.create(actorSystem, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(kafka.getBootstrapServers)
        .withGroupId("akka-stream-v0.1.0")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withStopTimeout(5.seconds)

    val sinkFuture =
      Consumer.plainSource(consumerSettings, Subscriptions.topics(secondTopic.name()))
        .log("consumed-event")
        .take(producedSet.size)
        .runWith(Sink.seq)
    val consumedSet = Await.result(sinkFuture, 30.seconds)
      .map(e => (e.key(), e.value()))
      .toSet

    assert(producedSet.size == consumedSet.size)
  }

}
