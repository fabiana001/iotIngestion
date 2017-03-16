package it.teamDigitale.kafkaProducers.main

import java.util.Properties
import java.util.concurrent.{ Executors, TimeUnit }

import com.typesafe.config.ConfigFactory
import it.teamDigitale.kafkaProducers.KafkaEventProducer
import it.teamDigitale.kafkaProducers.eventConverters.TorinoTrafficConverter
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory

/**
 * Created by fabiana on 13/03/17.
 */
object TorinoTrafficProducer extends App {

  //TODO we should add a redis db in the way to do not have redundant data if the service go down

  val logger = LoggerFactory.getLogger(this.getClass)

  var lastGeneratedTime: Option[Long] = None

  var config = ConfigFactory.load()
  val serializer = config.getString("spark-dataIngestion-example.kafka.serializer")
  val brokers = config.getString("spark-dataIngestion-example.kafka.bootstrapServers")
  val topic = config.getString("spark-dataIngestion-example.kafka.topic")

  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer)
  //props.put("enable.auto.commit", "true")
  props.put("enable.zookeeper", "true")
  //props.put("zookeeper.connect", zookeepers)
  //props.put("auto.offset.reset", "earliest")

  logger.info(s"Kafka Bootstrap Servers $brokers, topic $topic")

  val kafkaEventClient = new KafkaEventProducer[TorinoTrafficConverter](props, topic)

  val ex = Executors.newScheduledThreadPool(1)

  val task = new Runnable {
    def run(): Unit = {

      val time = lastGeneratedTime match {
        case None =>
          val time = kafkaEventClient.run(-1L)
          logger.info(s"Data analyzed for the time $time")
          println(s"Data analyzed for the time $time")
          time
        case Some(t) =>
          kafkaEventClient.run(t)

      }
      lastGeneratedTime = Some(time)
    }
  }
  ex.scheduleAtFixedRate(task, 2, 6, TimeUnit.SECONDS)

}
