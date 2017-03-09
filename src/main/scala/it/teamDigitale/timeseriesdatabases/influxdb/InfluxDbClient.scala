//package it.teamDigitale.timeseriesdatabases.influxdb
//
//import java.util.concurrent.TimeUnit
//
//import it.teamDigitale.avro.Event
//import org.apache.spark.rdd.RDD
//import org.influxdb.dto.Point
//import org.influxdb.{InfluxDB, InfluxDBFactory}
//
///**
//  * Created by fabiana on 06/03/17.
//  */
//class InfluxDbClient(user: String, password: String, url: String = "http://localhost:8086") {
//
//  //TODO define best values for FLUSH_PERIOD and BATCH_SIZE
//  val FLUSH_PERIOD = 100
//  final val BATCH_SIZE: Int = 2000
//
//  @transient lazy val influxDB: InfluxDB = InfluxDBFactory.connect(url, user, user)
//  influxDB.enableBatch(BATCH_SIZE, FLUSH_PERIOD, TimeUnit.MILLISECONDS)
//
//
//
//  def read(): RDD[Event] = ???
//
//  def write(rdd: RDD[Event]): Unit = {
//    rdd.foreach{ event =>
//
//
//      Point.measurement("event")
//        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//          .ad
//        .addField("cnt", cnt)
//        .addField("min", min)
//        .addField("max", max)
//        .addField("sum", sum)
//        .addField("last", last)
//        .build()
//
//
//    }
//
//
//
//    influxDB.write("timeseries", "default", p)
//
//
//  }
//
//  def close =  influxDB.close()
//}
