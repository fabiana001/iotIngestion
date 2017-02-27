package it.teamDigitale.consumers.sparkStreaming

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ ConsumerRecords, KafkaConsumer }
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._

/**
 * Created by fabiana on 27/02/17.
 */
object SimpleConsumer {
  def main(args: Array[String]): Unit = {

    var config = ConfigFactory.load()
    val serializer = config.getString("spark-opentsdb-exmaples.kafka.serializer")
    val brokers = config.getString("spark-opentsdb-exmaples.kafka.brokers")
    val topic = config.getString("spark-opentsdb-exmaples.kafka.topic")
    //val metric = config.getString("spark-opentsdb-exmaples.openTSDB.metric")
    val zookeepers = config.getString("spark-opentsdb-exmaples.zookeeper.host")

    val props = new Properties()

    props.put("bootstrap.servers", brokers)
    props.put("enable.auto.commit", "true")
    props.put("group.id", "test_consumer")

    val consumer = new KafkaConsumer(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumer.subscribe(Set(topic).asJava)

    while (true) {
      println(s"ciao $brokers")
      val records = consumer.poll(200)

      if (!records.isEmpty) {
        println("uffiiiiii")
        records.asScala.foreach(println)
      }
    }
  }

}
