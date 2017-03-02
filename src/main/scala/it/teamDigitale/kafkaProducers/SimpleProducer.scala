package it.teamDigitale.kafkaProducers

import java.time.LocalDateTime

import com.typesafe.config.ConfigFactory
import kafka.admin.AdminClient
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }

import scala.collection.JavaConverters._
import scala.util.Random
/**
 * Created by fabiana on 27/02/17.
 */
object SimpleProducer extends App {

  var con = ConfigFactory.load()
  val topic = con.getString("spark-opentsdb-exmaples.kafka.topic")
  val brokers = "193.204.187.132:9092"

  private val config: Map[String, Object] = Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringSerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringSerializer].getName,
    "enable.zookeeper" -> "true"
  )

  println(brokers)

  val adminClient = AdminClient.create(config)

  println(adminClient.bootstrapBrokers)

  val producer = new KafkaProducer[String, String](config.asJava)

  if (producer.partitionsFor(topic).isEmpty) {
    throw new RuntimeException("Kafka is not connected")
  }

  val rnd = new Random()
  val t = System.currentTimeMillis()
  while (true) {
    val runtime = LocalDateTime.now()
    val ip = s"192.168.2.${rnd.nextInt(255)}"
    val msg = s"$runtime www.example.com $ip"
    println(msg)
    val record = new ProducerRecord[String, String](topic, ip, msg)
    producer.send(record)
    Thread.sleep(100)

  }
}