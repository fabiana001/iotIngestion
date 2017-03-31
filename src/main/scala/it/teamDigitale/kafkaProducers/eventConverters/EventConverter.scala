package it.teamDigitale.kafkaProducers.eventConverters

/**
 * Created with <3 by Team Digitale.
 */
trait EventConverter {
  def convert(time: Long): (Long, Option[Seq[Array[Byte]]])
}
