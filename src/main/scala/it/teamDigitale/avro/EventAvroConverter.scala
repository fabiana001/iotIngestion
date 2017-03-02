package it.teamDigitale.avro

import java.io.File

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.Schema

/**
 * Created with <3 by Team Digitale.
 * It converts a standard Event into an avro format
 */
object EventAvroConverter {
  implicit private val specificAvroBinaryInjection: Injection[Event, Array[Byte]] = SpecificAvroCodecs.toBinary[Event]
  def convert(event: Event): Array[Byte] = specificAvroBinaryInjection(event)
}

object ExampleAvroConverter {
  val parser = new Schema.Parser()
  val USER_SCHEMA = scala.io.Source.fromFile("/Users/fabiana/Downloads/kafka-camel-example/src/main/scala/it/teamDigitale/avro/Event.avsc").mkString
  val schema = parser.parse(USER_SCHEMA)

  implicit private val specificAvroBinaryInjection: Injection[Example, Array[Byte]] = SpecificAvroCodecs.toBinary[Example]
  def convert(event: Example): Array[Byte] = specificAvroBinaryInjection(event)
}

