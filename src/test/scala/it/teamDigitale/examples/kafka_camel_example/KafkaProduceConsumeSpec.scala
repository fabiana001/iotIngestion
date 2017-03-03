///*
// * Copyright 2016 CGnal S.p.A.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package it.teamDigitale.examples.kafka_camel_example
//
//import java.io.{ File, FileNotFoundException, IOException }
//import java.net.ServerSocket
//import java.util.Properties
//
//import kafka.server.{ KafkaConfig, KafkaServer }
//import kafka.utils.{ MockTime, TestUtils, ZkUtils }
//import kafka.zk.EmbeddedZookeeper
//import org.I0Itec.zkclient.ZkClient
//import org.I0Itec.zkclient.serialize.ZkSerializer
//import org.apache.camel.builder.RouteBuilder
//import org.apache.camel.component.kafka.KafkaConstants
//import org.apache.camel.component.mock.{ MockComponent, MockEndpoint }
//import org.apache.camel.impl.DefaultCamelContext
//import org.apache.camel.{ Exchange, Processor }
//import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }
//
//import scala.language.existentials
//import scala.util.{ Failure, Random, Try }
//
//class KafkaProduceConsumeSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
//
//  val TIMEOUT = 1000
//
//  val POLL = 1000L
//
//  val ZKHOST = "127.0.0.1"
//
//  val NUMBROKERS = 4
//
//  val NUMPARTITIONS: Int = NUMBROKERS * 4
//
//  val BROKERHOST = "127.0.0.1"
//
//  val TOPIC_CAMEL = "test"
//
//  val TOPIC_CAMEL_ERRORS = "test_camel_error"
//
//  var zookeeperServer: Try[EmbeddedZookeeper] = Failure[EmbeddedZookeeper](new Exception(""))
//
//  var zkUtils: Try[ZkUtils] = Failure[ZkUtils](new Exception(""))
//
//  var kafkaServers: Array[Try[KafkaServer]] = new Array[Try[KafkaServer]](NUMBROKERS)
//
//  var logDir: Try[File] = Failure[File](new Exception("")) //constructTempDir("kafka-local")
//
//  def findAvailablePort(): Int = {
//    val port = try {
//      val socket = new ServerSocket(0)
//      try {
//        socket.getLocalPort
//      } finally {
//        socket.close()
//      }
//    } catch {
//      case e: IOException =>
//        throw new IllegalStateException(s"Cannot find available port: ${e.getMessage}", e)
//    }
//    port
//  }
//
//  def constructTempDir(dirPrefix: String): Try[File] = Try {
//    val rndrange = 10000000
//    val file = new File(System.getProperty("java.io.tmpdir"), s"$dirPrefix${Random.nextInt(rndrange)}")
//    if (!file.mkdirs())
//      throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath)
//    file.deleteOnExit()
//    file
//  }
//
//  def deleteDirectory(path: File): Boolean = {
//    if (!path.exists()) {
//      throw new FileNotFoundException(path.getAbsolutePath)
//    }
//    var ret = true
//    if (path.isDirectory)
//      path.listFiles().foreach(f => ret = ret && deleteDirectory(f))
//    ret && path.delete()
//  }
//
//  def makeZookeeperServer(): Try[EmbeddedZookeeper] = Try {
//    new EmbeddedZookeeper()
//  }
//
//  def makeZkUtils(zkServer: EmbeddedZookeeper): Try[ZkUtils] = Try {
//    val zkConnect = s"$ZKHOST:${zkServer.port}"
//    val zkClient = new ZkClient(zkConnect, Integer.MAX_VALUE, TIMEOUT, new ZkSerializer {
//      def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")
//
//      def deserialize(bytes: Array[Byte]): Object = new String(bytes, "UTF-8")
//    })
//    ZkUtils.apply(zkClient, isZkSecurityEnabled = false)
//  }
//
//  def makeKafkaServer(zkConnect: String, brokerId: Int): Try[KafkaServer] = Try {
//    logDir = constructTempDir("kafka-local")
//    val brokerPort = findAvailablePort()
//    val brokerProps = new Properties()
//    brokerProps.setProperty("zookeeper.connect", zkConnect)
//    brokerProps.setProperty("broker.id", s"$brokerId")
//    logDir.foreach(f => brokerProps.setProperty("log.dirs", f.getAbsolutePath))
//    brokerProps.setProperty("listeners", s"PLAINTEXT://$BROKERHOST:$brokerPort")
//    val config = new KafkaConfig(brokerProps)
//    val mockTime = new MockTime()
//    TestUtils.createServer(config, mockTime)
//  }
//
//  def shutdownKafkaServers(): Unit = {
//    kafkaServers.foreach(_.foreach(_.shutdown()))
//    kafkaServers.foreach(_.foreach(_.awaitShutdown()))
//    logDir.foreach(deleteDirectory)
//  }
//
//  def shutdownZookeeper(): Unit = {
//    zkUtils.foreach(_.close())
//    zookeeperServer.foreach(_.shutdown())
//  }
//  /*
//  override def beforeAll: Unit = {
//    zookeeperServer = makeZookeeperServer()
//    zkUtils = for {
//      zkServer <- zookeeperServer
//      zkUtils <- makeZkUtils(zkServer)
//    } yield zkUtils
//
//    for (i <- 0 until NUMBROKERS)
//      kafkaServers(i) = for {
//        zkServer <- zookeeperServer
//        kafkaServer <- makeKafkaServer(s"$ZKHOST:${zkServer.port}", i)
//      } yield kafkaServer
//  }
//
//  override def afterAll: Unit = {
//    shutdownKafkaServers()
//    shutdownZookeeper()
//  }
//    */
//  "Camel and Kafka" must {
//    "work well together" in {
//      // create topics
//      //zkUtils.foreach(AdminUtils.createTopic(_, TOPIC_CAMEL, NUMPARTITIONS, 1, new Properties(), RackAwareMode.Disabled))
//      //zkUtils.foreach(AdminUtils.createTopic(_, TOPIC_CAMEL_ERRORS, NUMPARTITIONS, 1, new Properties(), RackAwareMode.Disabled))
//      /*
//      val brokersUrl = kafkaServers.map(tk => s"localhost:${
//        tk.map[Int](server => {
//          server.config.listeners.head._2.port
//        }).getOrElse(throw new Exception("It shouldn't be here ..."))
//      }").mkString(",") */
//
//      val brokersUrl = "doc:9092,happy:9092,sleepy:9092,grumpy:9092"
//
//      val camelContext = new DefaultCamelContext()
//
//      val mockComponent = camelContext.resolveComponent("mock").asInstanceOf[MockComponent]
//
//      val mockOutEndpoint: MockEndpoint = mockComponent.createEndpoint("mock:out").asInstanceOf[MockEndpoint]
//
//      val mockErrorEndpoint: MockEndpoint = mockComponent.createEndpoint("mock:error").asInstanceOf[MockEndpoint]
//
//      camelContext.addRoutes(new RouteBuilder {
//        override def configure(): Unit = {
//
//          //producer route
//          from("direct:start").process(new Processor {
//            def process(exchange: Exchange): Unit = {
//              exchange.getIn().setHeader(KafkaConstants.KEY, s"${Random.nextInt(NUMPARTITIONS)}")
//            }
//          }).to(s"kafka:$brokersUrl?topic=$TOPIC_CAMEL&requestRequiredAcks=all")
//
//          //consumer route including error management
//          from(s"kafka:$brokersUrl?topic=$TOPIC_CAMEL&groupId=testing&autoOffsetReset=earliest&consumersCount=1&autoCommitEnable=false")
//            .doTry()
//            .process(new Processor {
//              def process(exchange: Exchange): Unit = {
//                var messageKey = ""
//                if (Option(exchange.getIn()).isDefined) {
//                  val message = exchange.getIn
//                  val partitionId = message.getHeader(KafkaConstants.PARTITION).asInstanceOf[Int]
//                  val topicName = message.getHeader(KafkaConstants.TOPIC).asInstanceOf[String]
//                  if (Option(message.getHeader(KafkaConstants.KEY)).isDefined)
//                    messageKey = message.getHeader(KafkaConstants.KEY).asInstanceOf[String]
//                  val data = message.getBody(classOf[String])
//                  if (data == "CIAO3") {
//                    throw new Exception("Wrong Message!")
//                  }
//                  println(s"topicName :: $topicName, partitionId :: $partitionId messageKey :: $messageKey $message :: $data")
//                }
//              }
//            })
//            .to(mockOutEndpoint)
//            .doCatch(classOf[Exception])
//            .to(mockErrorEndpoint)
//            .end()
//          ()
//        }
//      })
//
//      camelContext.start()
//
//      val to = 1000L
//      val template = camelContext.createProducerTemplate()
//      val numMessages = 1000
//      for (i <- 1 to numMessages)
//        template.sendBody("direct:start", s"CIAO1_$i")
//      mockOutEndpoint.setAssertPeriod(to)
//      mockOutEndpoint.expectedMessageCount(numMessages)
//      mockOutEndpoint.assertIsSatisfied()
//      mockOutEndpoint.reset()
//
//      template.sendBody("direct:start", "CIAO2")
//      mockOutEndpoint.setAssertPeriod(to)
//      mockOutEndpoint.expectedMessageCount(1)
//      mockOutEndpoint.assertIsSatisfied()
//      mockOutEndpoint.reset()
//
//      template.sendBody("direct:start", "CIAO3")
//      mockErrorEndpoint.setAssertPeriod(to)
//      mockErrorEndpoint.expectedMessageCount(1)
//      mockErrorEndpoint.assertIsSatisfied()
//      mockErrorEndpoint.reset()
//
//      camelContext.shutdown()
//
//      //remove topics
//      //zkUtils.foreach(AdminUtils.deleteTopic(_, TOPIC_CAMEL))
//      //zkUtils.foreach(AdminUtils.deleteTopic(_, TOPIC_CAMEL_ERRORS))
//
//    }
//  }
//
//}
