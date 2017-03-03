package it.teamDigitale.consumers.sparkStreaming

import java.beans.Introspector

import com.databricks.spark.avro._
import com.databricks.spark.avro.SchemaConverters
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.typesafe.config.ConfigFactory
import it.teamDigitale.avro.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.slf4j.{ Logger, LoggerFactory }
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.reflect.ClassTag
import scala.util.Success

/**
 * Created with <3 by Team Digitale.
 */
class EventConsumer(
    @transient val ssc: StreamingContext,
    topicSet: Set[String],
    kafkaParams: Map[String, Object]
) extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getEvents: DStream[Event] = {

    logger.info("Consumer is running")

    val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](topicSet, kafkaParams))
    inputStream.mapPartitions { rdd =>
      val specificAvroBinaryInjection: Injection[Event, Array[Byte]] = SpecificAvroCodecs.toBinary[Event]
      rdd.map { el =>
        val Success(event) = specificAvroBinaryInjection.invert(el.value())
        println(event)
        event
      }
    }
  }

  def getAvro: DStream[Array[Byte]] = {
    val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](topicSet, kafkaParams))
    inputStream.mapPartitions(rdd => rdd.map(_.value()))
  }

  def saveAsParquet(rdd: RDD[Event], spark: SparkSession, filename: String, attributes: List[String] = List()): Unit = {

    if (!rdd.isEmpty()) {
      import spark.implicits._
      val df = rdd.toDF()
      df.write.mode("append").partitionBy(attributes: _*).parquet(filename)
    }

  }

  def saveAsAvro(rdd: RDD[Array[Byte]], spark: SparkSession, filename: String, attributes: List[String] = List()): Unit = {
    if (!rdd.isEmpty()) {
      import spark.implicits._
      val df: DataFrame = rdd.toDF()
      df.write.mode("append").partitionBy(attributes: _*).avro(filename)
    }
  }

}

