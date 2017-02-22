package it.teamDigitale.examples.kafka_camel_example

import java.io.{FileInputStream, StringWriter}

import org.apache.camel.CamelContext
import org.apache.camel.scala.dsl.builder.ScalaRouteBuilder
import org.apache.commons.io.IOUtils
/**
  * Created by fabiana on 22/02/17.
  */
class WorkAroundRouting (override val context: CamelContext) extends ScalaRouteBuilder(context){


  val filename = "/Users/fabiana/Downloads/kafka-camel-example/articles"
  val articleStream = new FileInputStream(filename)

  val writer = new StringWriter()
  IOUtils.copy(articleStream, writer, "UTF-8")
  val theString = writer.toString()
  println(theString)

  val template = context.createProducerTemplate()
  template.sendBody("direct:data", articleStream)

  from("direct:data")
    .split(xpath("/articles/article"))
      .to("stream:out")
}
