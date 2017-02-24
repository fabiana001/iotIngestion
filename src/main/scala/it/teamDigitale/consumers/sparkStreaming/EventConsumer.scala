package it.teamDigitale.consumers.sparkStreaming

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.typesafe.config.ConfigFactory
import it.teamDigitale.avro.Event
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

import scala.util.Success

/**
 * Created by fabiana on 23/02/17.
 */
class EventConsumer(
    @transient val ssc: StreamingContext,
    topicSet: Set[String],
    kafkaParams: Map[String, String]
) extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)

  def run(): DStream[Event] = {

    logger.info("Consumer is running")
    assert(kafkaParams.contains("metadata.broker.list"))

    val inputStream: InputDStream[(Array[Byte], Array[Byte])] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicSet)

    inputStream.mapPartitions { rdd =>
      val specificAvroBinaryInjection: Injection[Event, Array[Byte]] = SpecificAvroCodecs.toBinary[Event]
      rdd.map { el =>
        val Success(event) = specificAvroBinaryInjection.invert(el._2)
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
    val props = Map("metadata.broker.list" -> brokers)

    val stream = new EventConsumer(ssc, Set(topic), props).run()

    stream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }

}