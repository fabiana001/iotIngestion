package it.teamDigitale.consumers.sparkStreaming

import it.teamDigitale.avro.{DataPoint, Event}
import it.teamDigitale.timeseriesdatabases.influxdb.InfluxDbClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

/**
  * Created by fabiana on 09/03/17.
  */
class InfluxdbConsumer (
                         ssc: StreamingContext,
                         topicSet: Set[String],
                         kafkaParams: Map[String, Object]
                       ) extends IoTConsumer[DataPoint](ssc, topicSet, kafkaParams) {

  def write(rdd: RDD[DataPoint], database:String, user: String, password: String, url: String= "http://localhost:8086"): Unit = {
    rdd.foreachPartition{partition =>
      if(!partition.isEmpty) {
        val influxDbClient = new InfluxDbClient(user, password, url)
        influxDbClient.write(partition, database)
      }
    }
  }

}