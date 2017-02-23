//package it.teamDigitale.camel
//
//import java.nio.ByteBuffer
//
//import it.teamDigitale.avro.Event
//import org.apache.camel.{Exchange, Processor}
//import scala.collection.JavaConverters._
//import scala.xml.XML
//
///**
//  * Created by fabiana on 22/02/17.
//  */
//class TorinoEventProcessor extends Processor{
//  import TorinoEventProcessor._
//  override def process(exchange: Exchange): Unit = {
//
//    val string = exchange.getIn.getBody(classOf[String])
//    val xml = XML.load(string)
//    xml.foreach{ string =>
//      val ftd_data = string \ "traffic_data" \ "FDT_data"
//      val lcd1 = (ftd_data \ "@lcd1").text
//      val road_LCD = (ftd_data \ "@Road_LCD").text.asInstanceOf[CharSequence]
//      val road_name = (ftd_data \ "@Road_name").text.asInstanceOf[CharSequence]
//      val offset = (ftd_data \ "@offset").text.asInstanceOf[CharSequence]
//      val attributes = Map(att_lcd1 -> lcd1,
//        att_road_LCD -> road_LCD,
//        att_road_name -> road_name,
//        att_offset -> offset
//      )
//
//
//      val event = new Event(
//        "sensor1".asInstanceOf[CharSequence],
//        System.currentTimeMillis(),
//        1,
//        "source",
//        "location",
//        "host",
//        ByteBuffer.wrap("raw data should go here".getBytes()),
//        attributes.asJava)
//
//    }
//  }
//}
//
//object TorinoEventProcessor{
//  val att_lcd1 = "FDT_data".asInstanceOf[CharSequence]
//  val att_road_LCD = "Road_LCD".asInstanceOf[CharSequence]
//  val att_road_name = "Road_name".asInstanceOf[CharSequence]
//  val att_offset = "offset".asInstanceOf[CharSequence]
//
//}