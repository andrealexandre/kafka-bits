package example

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

trait KakfaSpec extends AnyFlatSpec with BeforeAndAfterAll {

  val kafka: KafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))

  override protected def beforeAll(): Unit = kafka.start()

  override protected def afterAll(): Unit = kafka.stop()
}
