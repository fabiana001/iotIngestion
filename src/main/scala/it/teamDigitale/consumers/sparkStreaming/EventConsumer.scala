package it.teamDigitale.consumers.sparkStreaming

import it.teamDigitale.avro.Event
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * Created with <3 by Team Digitale.
 */
class EventConsumer(
    ssc: StreamingContext,
    topicSet: Set[String],
    kafkaParams: Map[String, Object]
) extends IoTConsumer[Event](ssc, topicSet, kafkaParams)
//) extends IoTConsumer(ssc, topicSet, kafkaParams)