package it.teamDigitale.consumers.sparkStreaming

import java.beans.Introspector

import com.databricks.spark.avro._
import com.databricks.spark.avro.SchemaConverters
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.typesafe.config.ConfigFactory
import it.teamDigitale.avro.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

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

  def getEvents(): DStream[Event] = {

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
    //val sqlContext: SQLContext = spark.sqlContext


    if (!rdd.isEmpty()) {

      val props = Introspector.getBeanInfo(classOf[Event]).getPropertyDescriptors
      props.foreach { prop =>
        println(prop.getDisplayName())
        println("\t" + prop.getReadMethod())
        println("\t" + prop.getWriteMethod());
      }

    //  val df: DataFrame = spark.createDataFrame(rdd, classOf[Event])
    //  val a: StructType = DataType.fromJson(Event.SCHEMA$.toString).asInstanceOf[StructType]
      //val pippo = sqlContext.createDataFrame(rdd.toJavaRDD(), classOf[Event])

      import spark.implicits._
      val df = rdd.toDF()
      df.write.partitionBy(attributes: _*).parquet(filename)
    }

  }

  def saveAsAvro(rdd: RDD[Array[Byte]], spark: SparkSession, filename: String, attributes: List[String] = List()): Unit = {
    val sqlContext = spark.sqlContext
    val schema = SchemaConverters.toSqlType(Event.SCHEMA$)
    val df = spark.createDataFrame(rdd.toJavaRDD(), classOf[Event]).toDF()
    df.write.partitionBy(attributes: _*).avro(filename)

  }

}

