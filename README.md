# iotIngestion
POC for Iot ingestion using Camel, Avro, Kafka and Spark Streaming

## Transform all data into an internal format (Event class) 
To be as efficient as possible in our implementation, we use Apache Avro to serialize/deserialize data from and to Kafka. 
In particular, inspired by [Osso-project](), we defined a common schema for any kind of input events.
This schema is general enough to fit most usage data types (aka event logs, data from mobile devices, sensors, etc.).

To generate a java class from an avro file, download avro-tools-{version}.jar from [here](http://mvnrepository.com/artifact/org.apache.avro/avro-tools/1.8.1) and exec:
> java -jar path/to/avro-tools-1.8.1.jar compile schema src/main/resources/Event.avsc ./src/main/scala
