package it.teamDigitale.avro

import org.scalatest.FunSuite
import org.json4s._
import org.json4s.native.JsonMethods._

/**
 * Created by Team Digitale.
 */
class EventAvroConverterSpec extends FunSuite {

  test("An event should be correctly serialized and deserialized") {

    val data =
      """{"id": "TorinoFDT",
        |"ts": 1488532860000,
        |"event_type_id": 1,
        |"source": "-1965613475",
        |"location": "45.06766-7.66662",
        |"service": "http://opendata.5t.torino.it/get_fdt",
        |"body": {"bytes": "<FDT_data period=\"5\" accuracy=\"100\" lng=\"7.66662\" lat=\"45.06766\" direction=\"positive\" offset=\"55\" Road_name=\"Corso Vinzaglio(TO)\" Road_LCD=\"40201\" lcd1=\"40202\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://www.5t.torino.it/simone/ns/traffic_data\">\n    <speedflow speed=\"20.84\" flow=\"528.00\"/>\n  </FDT_data>"},
        |"attributes": {"period": "5", "offset": "55", "Road_name": "Corso Vinzaglio(TO)", "Road_LCD": "40201", "accuracy": "100", "FDT_data": "40202", "flow": "528.00", "speed": "20.84", "direction": "positive"}
        |}""".stripMargin
    implicit val formats = DefaultFormats

    val event = parse(data, true).extract[Event]
    val avro = AvroConverter.convertEvent(event)

    assert(event.id.getOrElse("") == "TorinoFDT")
    assert(avro.length > 0)
  }

}
