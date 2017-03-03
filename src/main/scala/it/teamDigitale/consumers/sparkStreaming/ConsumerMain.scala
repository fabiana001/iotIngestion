package it.teamDigitale.consumers.sparkStreaming

import java.nio.ByteBuffer

import collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import it.teamDigitale.avro.Event
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ DataType, StructType }
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
import org.slf4j.{ Logger, LoggerFactory }

/**
 * Created with <3 by Team Digitale.
 */
object ConsumerMain {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("spark-simpleEvent-test")

    var config = ConfigFactory.load()

    val filename = config.getString("spark-dataIngestion-example.hdfs.filename")

    val test = args match {
      case Array(testMode: String) =>
        logger.info(s"kafka: ${config.getString("spark-dataIngestion-example.kafka.bootstrapServers")}")
        logger.info(s"zookeeper: ${config.getString("spark-dataIngestion-example.zookeeper.host")}")
        testMode.toBoolean
      case _ => true
    }

    if (test)
      spark.master("local[*]")

    val sparkSession = spark
      .getOrCreate()

    val sparkConf = sparkSession.sparkContext
    implicit val ssc = new StreamingContext(sparkConf, Minutes(3))

    val topic = ConfigFactory.load().getString("spark-dataIngestion-example.kafka.topic")
    val servers = ConfigFactory.load().getString("spark-dataIngestion-example.kafka.bootstrapServers")
    val deserializer = ConfigFactory.load().getString("spark-dataIngestion-example.kafka.deserializer")

    val props = Map(
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      //"auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> "test_event_consumer"
    )

    val eventConsumer = new EventConsumer(ssc, Set(topic), props)
    val stream = eventConsumer.getEvents

    stream.print(100)
    stream.foreachRDD(rdd => eventConsumer.saveAsParquet(rdd, spark.getOrCreate(), filename, List("ts")))

    ssc.start()
    ssc.awaitTermination()
  }
}
