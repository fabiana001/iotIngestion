package it.teamDigitale

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Try

/**
  * Created by fabiana on 23/02/17.
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
