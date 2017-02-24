package it.teamDigitale.kafkaProducers.eventConverters

import scala.collection.immutable.Seq

/**
  * Created with <3 by Team Digitale.
  */
trait EventConverter {
  def convert(time: Long): (Long, Option[Seq[Array[Byte]]])
}
