package it.teamDigitale.avro

import java.io.File

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.Schema

/**
 * Created with <3 by Team Digitale.
 * It converts a standard Event into an avro format
 */
object AvroConverter {
  implicit private val specificEventAvroBinaryInjection: Injection[Event, Array[Byte]] = SpecificAvroCodecs.toBinary[Event]
  implicit private val specificDPAvroBinaryInjection: Injection[DataPoint, Array[Byte]] = SpecificAvroCodecs.toBinary[DataPoint]
  def convertEvent(event: Event): Array[Byte] = specificEventAvroBinaryInjection(event)
  def convertDataPoint(dp: DataPoint): Array[Byte] = specificDPAvroBinaryInjection(dp)

}
