package it.teamDigitale.avro

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

/**
  * Created by fabiana on 23/02/17.
  */
object EventAvroConverter {
  implicit private val specificAvroBinaryInjection: Injection[Event, Array[Byte]] = SpecificAvroCodecs.toBinary[Event]
  def convert(event: Event): Array[Byte] = specificAvroBinaryInjection(event)
}
