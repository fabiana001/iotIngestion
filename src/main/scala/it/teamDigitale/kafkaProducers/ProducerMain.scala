package it.teamDigitale.kafkaProducers

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory


/**
  * Created with <3 by Team Digitale
  * Example of a Kafka producer for Torino Iot
  */
object ProducerMain extends App {

  //TODO we should add a redis db in the way to do not have redundant data if the service go down

  val logger = LoggerFactory.getLogger(this.getClass)

  var lastGeneratedTime: Option[Long] = None

  var config = ConfigFactory.load()
  val serializer = config.getString("spark-opentsdb-exmaples.kafka.serializer")
  val brokers = config.getString("spark-opentsdb-exmaples.kafka.brokers")
  val topic = config.getString("spark-opentsdb-exmaples.kafka.topic")
  //val metric = config.getString("spark-opentsdb-exmaples.openTSDB.metric")
  val zookeepers = config.getString("spark-opentsdb-exmaples.zookeeper.host")

  val props = new Properties()

  //brokers are sequences of ip:port (e.g., "localhost:9092, 193.204.187.22:9092")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer)
  props.put("zookeeper.connect", zookeepers)

  val kafkaClient = new KafkaEventProducer(props, topic)

  val ex = Executors.newScheduledThreadPool(1)

  val task = new Runnable {
    def run() = {
      lastGeneratedTime match {
        case None =>
          val (time, avro)= TorinoTrafficProducer.run(-1L)
          //val pippo: Seq[Array[Byte]] = avro.get
          lastGeneratedTime = Some(time)

          avro.getOrElse(Seq.empty[Array[Byte]]).foreach(kafkaClient.exec(_))

          logger.info(s"Data analyzed for the time ${lastGeneratedTime.getOrElse("")}")

        case Some(t) =>
          val (time, avro) = TorinoTrafficProducer.run(t)
          lastGeneratedTime = Some(time)
          ()
      }
    }
  }
  ex.scheduleAtFixedRate(task, 2, 5, TimeUnit.SECONDS)

}
