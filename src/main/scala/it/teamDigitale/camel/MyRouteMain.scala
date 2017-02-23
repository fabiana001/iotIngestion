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
//import org.apache.camel.impl.DefaultCamelContext
//import org.apache.camel.main.Main
//import org.apache.camel.scala.dsl.builder.RouteBuilderSupport
//
///**
// * A Main to run Camel with MyRouteBuilder
// */
//object MyRouteMain extends App {
//
//    // create the CamelContext
//    val context = new DefaultCamelContext()
//    // add our route using the created CamelContext
//    context.addRoutes(new MyRouteBuilder)
//    // must use run to start the main application
//   context.start()
//   Thread.sleep(100000L)
//   context.stop()
//
//}
//
