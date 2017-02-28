import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt._

name := "iotIngestion"

organization := "it.teamDigitale"

version := "1.0"

val assemblyName = "iotIngestion"

scalaVersion in ThisBuild := "2.11.8"

scalariformSettings

scalastyleFailOnError := true

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code",
  "-Xfuture"
)

//wartremoverErrors ++= Seq(
//  //Wart.Any,
//  Wart.Any2StringAdd,
//  //  Wart.AsInstanceOf,
//  //Wart.DefaultArguments,
//  Wart.EitherProjectionPartial,
//  Wart.Enumeration,
//  //  Wart.Equals,
//  Wart.ExplicitImplicitTypes,
//  Wart.FinalCaseClass,
//  Wart.FinalVal,
//  Wart.ImplicitConversion,
//  //Wart.IsInstanceOf,
//  Wart.JavaConversions,
//  Wart.LeakingSealed,
//  Wart.ListOps,
//  Wart.MutableDataStructures,
//  Wart.NoNeedForMonad,
//  //  Wart.NonUnitStatements,
//  //Wart.Nothing,
//  Wart.Null,
//  //Wart.Option2Iterable,
//  //Wart.OptionPartial,
//  Wart.Overloading,
//  Wart.Product,
//  Wart.Return,
//  //Wart.Serializable,
//  //  Wart.Throw,
//  Wart.ToString,
//  Wart.TryPartial,
//  //  Wart.Var,
//  Wart.While
//)

val kafkaVersion = "0.10.1.1"
val camelVersion = "2.18.1"
val scalaxmlVersion = "1.0.6"
val apacheLog4jVersion = "2.7"
val scalaTestVersion = "3.0.0"
val sparkVersion = "2.1.0"
val sparkAvroVersion = "3.2.0"

resolvers ++= Seq(
  Resolver.mavenLocal
)

/**
  * unless Spark and hadoop get in  trouble about signed jars.
  */
val hadoopHBaseExcludes =
  (moduleId: ModuleID) => moduleId.
    excludeAll(ExclusionRule(organization = "org.mortbay.jetty")).
    excludeAll(ExclusionRule(organization = "org.eclipse.jetty")).
    excludeAll(ExclusionRule(organization = "javax.servlet"))


/**
* when used inside the IDE they are imported with scope "compile",
* Otherwise when submitted with spark_submit they are  "provided"
*/
def providedOrCompileDependencies(scope: String = "compile"): Seq[ModuleID] = Seq(
  //For spark Streaming Dependencies
  hadoopHBaseExcludes("com.databricks" %% "spark-avro" % sparkAvroVersion % "compile"),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-core" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % scope))

val commonDependencies = Seq(
  //For Camel Dependencies
  "org.apache.camel" % "camel-core" % camelVersion % "compile",
  "org.apache.camel" % "camel-scala" % camelVersion % "compile",
  "org.apache.camel" % "camel-http4" % camelVersion % "compile",
  "org.apache.camel" % "camel-stream" % camelVersion % "compile",
  "org.scala-lang.modules" %% "scala-xml" % scalaxmlVersion % "compile",
  "org.apache.camel" % "camel-kafka" % camelVersion % "compile" exclude("org.apache.kafka", "kafka-clients"),

  "com.typesafe" % "config" % "1.0.2",

  //avro dependencies
  "org.apache.avro" % "avro" % "1.8.1",
  "com.twitter" %% "bijection-avro" % "0.9.2",
  "com.twitter" %% "bijection-core" % "0.9.2",

  //Logging Dependencies
 // "org.apache.logging.log4j" % "log4j-api" % apacheLog4jVersion % "compile",
 // "org.apache.logging.log4j" % "log4j-core" % apacheLog4jVersion % "compile",
 // "org.apache.logging.log4j" % "log4j-slf4j-impl" % apacheLog4jVersion % "compile",
  //Kafka Dependencies
  "org.apache.kafka" %% "kafka" % kafkaVersion % "compile"
    exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
    exclude("javax.jms", "jms"),
  "org.apache.kafka" %% "kafka" % kafkaVersion % "test" classifier "test",
  "org.apache.kafka" % "kafka-clients" % kafkaVersion % "compile",
  "org.apache.kafka" % "kafka-clients" % kafkaVersion % "test" classifier "test",
  "org.apache.commons" % "commons-io" % "1.3.2" % "test",

  //Test Dependencies
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)



lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies ++ providedOrCompileDependencies()
  ).
  enablePlugins(AutomateHeaderPlugin, JavaAppPackaging, DockerPlugin, UniversalPlugin).
  disablePlugins(AssemblyPlugin)
  //enablePlugins(AssemblyPlugin)

lazy val projectAssembly = (project in file("assembly")).
  settings(
    mainClass in assembly := Some("it.teamDigitale.kafkaProducers.SimpleProducer"),
    assemblyJarName in assembly := s"$assemblyName-${version.value}.jar"
  ) dependsOn root

scriptClasspath ++= Seq(s"$assemblyName-${version.value}.jar")

    
