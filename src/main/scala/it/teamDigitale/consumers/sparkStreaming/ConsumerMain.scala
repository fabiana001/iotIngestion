package it.teamDigitale.consumers.sparkStreaming

import java.nio.ByteBuffer

import collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import it.teamDigitale.avro.Event
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ DataType, StructType }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.slf4j.LoggerFactory


/**
 * Created by fabiana on 28/02/17.
 */
object ConsumerMain {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("spark-simpleEvent-test")

    var config = ConfigFactory.load()

    val test = args match {
      case Array(testMode: String) =>
        logger.info(s"kafka: ${config.getString("spark-opentsdb-exmaples.kafka.bootstrapServers")}")
        logger.info(s"zookeeper: ${config.getString("spark-opentsdb-exmaples.zookeeper.host")}")
        testMode.toBoolean

      case _ => true
    }

    if (test)
      spark.master("local[*]")

    val sparkSession = spark
      //.enableHiveSupport()
      .getOrCreate()

    val sparkConf = sparkSession.sparkContext
    implicit val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
    val servers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.bootstrapServers")
    val deserializer = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.deserializer")

    val props = Map(
      // "metadata.broker.list" -> brokers,
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      //"auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> "test_event_consumer"
    )

    val eventConsumer = new EventConsumer(ssc, Set(topic), props)
    val stream = eventConsumer.getEvents()

    stream.print(100)
    stream.foreachRDD(rdd => eventConsumer.saveAsParquet(rdd, spark.getOrCreate(), "pippo"))

    //eventConsumer.saveAsParquet(ev, spark.getOrCreate(), "ciao", List())

    ssc.start()
    ssc.awaitTermination()
  }
}
