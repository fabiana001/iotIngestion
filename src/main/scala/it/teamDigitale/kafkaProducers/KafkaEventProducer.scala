package it.teamDigitale.kafkaProducers

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Try

/**
  * Created with <3 by Team Digitale
  *
  * It sends events to a kafka queue
  */
class KafkaEventProducer(props: Properties, topic: String) {

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

  def exec(bytes:Array[Byte]): Try[Unit] = Try{
    val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)
    producer.send(message)
    ()
  }

  def close(): Unit = producer.close()


}
