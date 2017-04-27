package it.teamDigitale.kafkaProducers.eventConverters

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import it.teamDigitale.avro.{AvroConverter, DataPoint}
import org.apache.logging.log4j.scala.Logging
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Await, Future}
import scala.io.BufferedSource
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

/**
 * Created by fabiana on 27/03/17.
 */
class FirenzeTrafficConverter extends EventConverter with Logging {
  import FirenzeTrafficConverter._

  override def convert(lastExtractedData: Map[String, Long] = Map()): (Map[String, Long], Option[Seq[Array[Byte]]]) = {

    val tryReq = Try(Await.result(collectSuccessHttpRequest(urls), FiniteDuration(180, TimeUnit.SECONDS)))
    tryReq match {
      case Success(list) =>

        val dataPoints: Seq[((String, Long), List[DataPoint])] = list.flatMap {
          case (sensor, body) =>
            val time = lastExtractedData.getOrElse(sensor.url, -1L)
            val d = extractDataPoints(sensor, body, time)

            d match {
              case Nil =>
                None
              case _ =>
                val sortedDataPoint = d.sortBy(_.ts)
                val lastDate = sortedDataPoint.last.ts

                Some(((sensor.url, lastDate), d))
            }
        }

        val newLastExtractedData = dataPoints
          .map {
            case ((url, time), _) =>
              (url, time)
          }
          .toMap
        val dpConverted = dataPoints.flatMap { x => x._2 }.map(d => AvroConverter.convertDataPoint(d))
        val optionDpConverted = dpConverted match {
          case Nil => None
          case _ => Some(dpConverted)
        }
        val newMap = updateLastExtractedTimeMap(lastExtractedData, newLastExtractedData)

        (newMap, optionDpConverted)

      case Failure(ex) =>
        logger.error(s"${ex.getMessage}\n ${ex.getStackTrace}")

        (lastExtractedData, None)
    }
  }

  private def updateLastExtractedTimeMap(oldMap: Map[String, Long], newMap: Map[String, Long]): Map[String, Long] = {

    oldMap.isEmpty match {
      case true => newMap
      case false =>
        oldMap.keys.map { k =>
          val oldValue: Long = oldMap(k)
          (k, newMap.getOrElse(k, oldValue))
        }.toMap
    }

  }

  def extractDataPoints(sensor: FirenzeSensor, corpus: BufferedSource, lastExtractedTime: Long): List[DataPoint] = {

    val data = fromJson[SensorFormat](corpus.mkString)
    if (data.rows.isEmpty)
      logger.debug(s"No datapoints extracted for the sensor ${sensor.url}")
    val locString = s"${sensor.lat}-${sensor.lon}"

    data.rows
      .map { row =>
        (getTime(row), row.value, row)
      }
      .filter {
        case (time, value, rowData) =>
          time != -1 & time > lastExtractedTime
      }
      .map {
        case (time, value, rowData) =>

          new DataPoint(
            version = 0L,
            id = Some(measure), //here we should put something as "traffic"
            ts = time,
            event_type_id = measure.hashCode,
            location = locString,
            host = host,
            service = sensor.url,
            body = Some(toJson(rowData).getBytes()),
            tags = Map("name" -> sensor.name),
            values = Map("flow" -> value.toDouble)
          )
      }.toList

  }

  private def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.readValue[T](json)
  }

  private def toJson(value: Data): String = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.writeValueAsString(value)
  }

  private def getTime(row: Data): Long = {
    val year = row.key.apply(2).toInt
    val month = row.key.apply(3).toInt
    val day = row.key.apply(4).toInt
    val hour = row.key.apply(5).toInt
    val minute = row.key.apply(6).toInt

    val timeTry = Try {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm")
      val parsedDate = dateFormat.parse(s"$year-$month-$day $hour:$minute")
      val millisec = parsedDate.getTime()
      millisec
    }

    timeTry match {
      case Success(time) => time
      case Failure(ex) =>
        logger.error(ex.getMessage)
        -1
    }
  }

}

object FirenzeTrafficConverter {

  case class FirenzeSensor(url: String, name: String, lat: Double, lon: Double)
  //case class Data(key: Array[Any], value: Int)
  case class Data(key: Array[String], value: Int)
  case class SensorFormat(rows: Array[Data])

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val urlCavourCentro = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_cavour_centro.json", "Cavour centro", 11.256386, 43.775531)
  val urlCavourPeriferia = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_cavour_periferia.json", "Cavour periferia", 11.256386, 43.775531)
  val urlDemidoffCentro = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_demidoff_centro.json", "Demidoff centro", 11.226857, 43.788286)
  val urlDemidoffPeriferia = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_demidoff_periferia.json", "Demidoff periferia", 11.226857, 43.788286)
  val urlReginaldoGiulianiCentro = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_reginaldo_giuliani-centro.json", "Reginaldo Giuliani centro", 11.233707, 43.809224)
  val urlReginaldoGiulianiPeriferia = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_reginaldo_giuliani_periferia.json", "Reginaldo Giuliani periferia", 11.233707, 43.809224)
  val urlSeneseCentro = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_senese_centro.json", "Senese centro", 11.234132, 43.748638)
  val urlSenesePeriferia = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_senese_periferia.json", "Senese periferia", 11.234132, 43.748638)
  val urlVillamagnaCentro = FirenzeSensor("http://opendata.comune.fi.it/od/sensore_villamagna_centro.json", "Villamagna centro", 11.282637, 43.763586)

  val urls = Array(
    urlCavourCentro,
    urlCavourPeriferia,
    urlDemidoffCentro,
    urlDemidoffPeriferia,
    urlReginaldoGiulianiCentro,
    urlReginaldoGiulianiPeriferia,
    urlSeneseCentro,
    urlSenesePeriferia,
    urlVillamagnaCentro
  )

  val measure = "opendata.firenze.flow"
  val host = "http://opendata.comune.fi.it"

  def collectSuccessHttpRequest(urls: Array[FirenzeSensor]): Future[List[(FirenzeSensor, BufferedSource)]] = {

    val httpRequests: List[Future[Try[(FirenzeSensor, BufferedSource)]]] = urls
      .map(sensor => Future(Try((sensor, scala.io.Source.fromURL(sensor.url, "UTF-8")))))
      .toList

    val successRequests = httpRequests
      .map(_.map {
        case Success(x) => Some(x)
        case Failure(ex) =>
          logger.error(s"Failed Http request: $ex")
          None
      })

    Future.sequence(successRequests).map(_.flatten)
  }

  def futureToFutureTry[T](f: Future[T]): Future[Try[T]] = f.map(Success(_)).recover { case x => Failure(x) }

}
