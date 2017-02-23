///*
// * Copyright 2017 TeamDigitale
// *
// */
//
//package it.teamDigitale.examples.kafka_camel_example
//
//import org.apache.camel.builder.RouteBuilder
//import org.apache.camel.builder.xml.Namespaces
//import org.apache.camel.scala.dsl.builder.ScalaRouteBuilder
//import org.apache.camel.{CamelContext, Exchange}
//
//import scala.xml.XML
//
//class MyRouteBuilder extends RouteBuilder {
//
//  // an example of a Processor method
//  val myProcessorMethod: (Exchange) => Unit = (exchange: Exchange) => {
//    val string = exchange.getIn().getBody(classOf[String])
//
//    val xml = XML.loadString(string)
//    val data = xml.map { string =>
//      val ftd_data = string \ "traffic_data" \ "FDT_data"
//      val lcd1 = (ftd_data \ "@lcd1").text
//      val road_LCD = (ftd_data \ "@Road_LCD").text
//      val road_name = (ftd_data \ "@Road_name").text
//      val offset = (ftd_data \ "@offset").text
//      (ftd_data, lcd1, road_LCD, road_name, offset)
//    }
//      //println("*****************1ciao" + string + "\n STo dentro")
//      //exchange.getIn.setBody("PIPPO")
//    }
//
//
//    //  // a route using Scala blocks
//    //  from("timer://foo?period=5000") ==> {
//    //    process(myProcessorMethod)
//    //    to("log:block")
//    //    ()
//    //  }
//
//
//  override def configure(): Unit = {
//    val namespace = "http://www.5t.torino.it/simone/ns/traffic_data"
//    val ns = new Namespaces("c", namespace)
//
//    // torino is the name of the Timer object, which is created and shared across endpoints.
//    // So if you use the same name for all your timer endpoints, only one Timer object and thread will be used.
//
//    from("timer://torino?period=5000")
//      .to("http4://opendata.5t.torino.it/get_fdt")
//      .log(s"Split by FDT_data Element ")
//      .split().xpath("/c:traffic_data/c:FDT_data",ns)
//      .to("log:block")
//      //.to("stream:out")
//      ()
//  }
//}