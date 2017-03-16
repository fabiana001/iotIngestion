package it.teamDigitale.timeseriesdatabases.influxdb

import java.util
import java.util.concurrent.TimeUnit

import collection.JavaConverters._
import it.teamDigitale.avro.{DataPoint, Event}
import org.apache.spark.rdd.RDD
import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.dto.{BatchPoints, Point}
import org.influxdb.{InfluxDB, InfluxDBFactory}

/**
  * Created by fabiana on 06/03/17.
  */
class InfluxDbClient(user: String, password: String, url: String = "http://localhost:8086") {

  //TODO define best values for FLUSH_PERIOD and BATCH_SIZE
  val FLUSH_PERIOD = 100
  final val BATCH_SIZE: Int = 2000

  @transient val influxDB: InfluxDB = InfluxDBFactory
    .connect(url, user, password)
    .enableBatch(BATCH_SIZE, FLUSH_PERIOD, TimeUnit.MILLISECONDS)

  def read(): RDD[DataPoint] = ???


  def write(rdd: Iterator[DataPoint], database: String = "timeseries"): Unit = {

      val batchPoints = BatchPoints
        .database(database)
        .tag("async", "true")
        .consistency(ConsistencyLevel.ONE)
        .retentionPolicy("autogen")
        .build()

      rdd.foreach{ dp =>
        val fields: util.Map[String, Object] = dp.values.map { case (k, v) => (k, v.asInstanceOf[Object]) }.asJava
        val point = Point.measurement(dp.host)
          .time(dp.ts, TimeUnit.MILLISECONDS)
          .fields(fields)
          .tag(dp.tags.asJava)
          .build()
        batchPoints.point(point)
      }
      influxDB.write(batchPoints)
  }

  def close() =  influxDB.close()
}
