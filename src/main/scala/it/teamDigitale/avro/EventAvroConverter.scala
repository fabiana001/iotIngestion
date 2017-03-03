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
