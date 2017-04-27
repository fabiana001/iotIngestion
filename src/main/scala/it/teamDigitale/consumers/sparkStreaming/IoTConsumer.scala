package it.teamDigitale.consumers.sparkStreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.slf4j.{Logger, LoggerFactory}
import com.databricks.spark.avro._
import com.databricks.spark.avro.SchemaConverters
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scala.util.Success

/**
 * Created by fabiana on 09/03/17.
 */
abstract class IoTConsumer[T <: SpecificRecordBase: ClassTag](
    @transient val ssc: StreamingContext,
    topicSet: Set[String],
    kafkaParams: Map[String, Object]
) extends Serializable with Logging{

  def getStream: DStream[T] = {
    logger.info("Consumer is running")

    val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](topicSet, kafkaParams))
    inputStream.mapPartitions { rdd =>
      val specificAvroBinaryInjection: Injection[T, Array[Byte]] = SpecificAvroCodecs.toBinary[T]
      rdd.map { el =>
        val Success(data) = specificAvroBinaryInjection.invert(el.value())
        println(data)
        data
      }
    }
  }

  def getAvro: DStream[Array[Byte]] = {
    val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](topicSet, kafkaParams))
    inputStream.mapPartitions(rdd => rdd.map(_.value()))
  }

  def saveAsAvro(rdd: RDD[Array[Byte]], spark: SparkSession, filename: String, attributes: List[String] = List()): Unit = {
    if (!rdd.isEmpty()) {
      import spark.implicits._
      val df: DataFrame = rdd.toDF()
      df.write.mode("append").partitionBy(attributes: _*).avro(filename)
    }
  }

  def saveAsParquet[Z <: Product: TypeTag](rdd: RDD[Z], spark: SparkSession, filename: String, attributes: List[String] = List()): Unit = {
    if (!rdd.isEmpty()) {
      import spark.implicits._
      val df = spark.createDataFrame(rdd)
      df.write.mode("append").partitionBy(attributes: _*).parquet(filename)
    }

  }
}
