package it.teamDigitale.kafkaProducers

import java.io.Serializable
import java.util.Properties

import it.teamDigitale.kafkaProducers.eventConverters.EventConverter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.reflect.{ClassTag, classTag}
import scala.util.{Failure, Try}

/**
 * Created with <3 by Team Digitale
 *
 * It sends events to a kafka queue
 */
class KafkaEventProducer[T <: EventConverter: ClassTag](props: Properties, topic: String) {

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
  val logger = LoggerFactory.getLogger(this.getClass)
  val converter: T = classTag[T].runtimeClass.newInstance().asInstanceOf[T with Serializable]

  def run(time: Long): Long = {

    val (newTime, avro) = converter.convert(time)
    avro.getOrElse(Seq.empty[Array[Byte]]).foreach{data =>
      exec(data) match {
        case Failure(ex) => logger.error(s"${ex.getStackTrace}")
        case _ =>
      }
    }
    newTime
  }

  def exec(bytes: Array[Byte]): Try[Unit] = Try {
    val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)
    producer.send(message)
    ()
  }

  def close(): Unit = producer.close()

}
