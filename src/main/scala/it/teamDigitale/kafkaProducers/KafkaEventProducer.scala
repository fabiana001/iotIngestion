package it.teamDigitale.kafkaProducers

import java.io.Serializable
import java.util.Properties

import it.teamDigitale.kafkaProducers.eventConverters.EventConverter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.logging.log4j.scala.Logging
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.{ClassTag, classTag}
import scala.util.{Failure, Try}

/**
 * Created with <3 by Team Digitale
 *
 * It sends events to a kafka queue
 */
class KafkaEventProducer[T <: EventConverter: ClassTag](props: Properties, topic: String) extends Logging{

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
  val converter: T = classTag[T].runtimeClass.newInstance().asInstanceOf[T with Serializable]

  def run(timeMap: Map[String, Long] = Map.empty): Map[String, Long] = {

    val (newTimeMap, avro) = converter.convert(timeMap)
    avro.getOrElse(Seq.empty[Array[Byte]]).foreach { data =>
      exec(data) match {
        case Failure(ex) => logger.error(s"${ex.getStackTrace}")
        case _ =>
      }
    }

    if (avro.nonEmpty) {
      logger.info(s"Extracted ${avro.get.size} elements")
      producer.flush()
    }
    newTimeMap
  }

  def exec(bytes: Array[Byte]): Try[Unit] = Try {
    val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)
    producer.send(message)
    ()
  }

  def close(): Unit = producer.close()

}
