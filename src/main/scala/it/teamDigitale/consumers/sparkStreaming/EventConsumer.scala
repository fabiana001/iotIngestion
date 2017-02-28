package it.teamDigitale.consumers.sparkStreaming

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.typesafe.config.ConfigFactory
import it.teamDigitale.avro.Event
import kafka.serializer.DefaultDecoder
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable
import scala.util.Success

/**
 * Created by fabiana on 23/02/17.
 */
class EventConsumer(
    @transient val ssc: StreamingContext,
    topicSet: Set[String],
    kafkaParams: Map[String, Object]
) extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)

  def run(): DStream[Event] = {

    logger.info("Consumer is running")
    //assert(kafkaParams.contains("metadata.broker.list"))

    //val inputStream: InputDStream[(Array[Byte], Array[Byte])] = //KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicSet)
    val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](topicSet, kafkaParams))
    inputStream.mapPartitions { rdd =>
      val specificAvroBinaryInjection: Injection[Event, Array[Byte]] = SpecificAvroCodecs.toBinary[Event]
      rdd.map { el =>
        val Success(event) = specificAvroBinaryInjection.invert(el.value())
        event
      }
    }
  }

}

object EventConsumerMain {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().
      setAppName("spark-simpleEvent-test")
    //set("spark.io.compression.codec", "lzf")

    var config = ConfigFactory.load()

    val test = args match {
      case Array(testMode: String) =>
        logger.info(s"kafka: ${config.getString("spark-opentsdb-exmaples.kafka.brokers")}")
        logger.info(s"zookeeper: ${config.getString("spark-opentsdb-exmaples.zookeeper.host")}")
        testMode.toBoolean

      case _ => true
    }

    if (test)
      sparkConf.setMaster("local[*]")

    implicit val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
    val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.brokers")
    val servers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.bootstrapServers")
    val deserializer = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.deserializer")

    val props: Map[String, Object] = Map(
      // "metadata.broker.list" -> brokers,
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      //"auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> "test_event_consumer"
    )

    val stream = new EventConsumer(ssc, Set(topic), props).run()

    //    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](Set(topic), props))
    //    val pippo = stream.map { record =>
    //      record.value()
    //    }
    //    pippo.print(100)

    stream.print(100)
    //stream.foreachRDD(rdd => rdd.foreach(println(_)))

    ssc.start()
    ssc.awaitTermination()
  }

}