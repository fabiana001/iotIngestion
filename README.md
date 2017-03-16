# IotIngestion
POC for Iot ingestion using Avro, Kafka and Spark Streaming

## Requirements
* [Zookeeper 3.4.x](https://zookeeper.apache.org/releases.html#download)
* [Kafka 0.10.x](https://kafka.apache.org/downloads) 
* [InfluxDB 1.2.x](https://portal.influxdata.com/downloads)
* [Grafana 4.2.x](https://grafana.com/grafana/download)

We provide a [docker compose file](https://github.com/fabiana001/iotIngestion/blob/master/dockers/docker-compose.yml) for running the above services.
## Transform all data into an internal format (i.e. DataPoint) 
To be as efficient as possible, we use Apache Avro to serialize/deserialize data from and to Kafka. 
In particular, inspired by [Osso-project](http://www.osso-project.org/), we defined a common schema, called [DataPoint](https://github.com/fabiana001/iotIngestion/blob/master/src/main/scala/it/teamDigitale/avro/DataPoint.avsc) for any kind of input events.
This schema is general enough to fit most usage data types (aka event logs, data from mobile devices, sensors, etc.).

A generic DataPoint is defined as follows:
* Version (long) - The Osso event format version.
* Event type ID (int) - A numeric event type.
* Event ID (string) - An ID that uniquely identifies an event.
* Timestamp (long) - A millisecond-accurate epoch timestamp indicating the moment the event occurred.
* Location (string) - The location that generated the event.
* Host (string) - The host or device that generated the event (e.g. http://opendata.5t.torino.it/get_fdt).
* Service (string) - The service that generate the event (e.g. the uri who provides data such as ).
* Body (byte array) - The body or “payload” of the event.
* Tags (Map[String, String]) - the combination of a tag key-value (e.g. via=Roma, offset=500) 
* Values (Map[String, Double]) - actual measured values (e.g. free_parking=134.0)

## Compile the code 
 #### 1. Generate the DataPoint class
To generate a java class from an avro file, download avrohugger-tools-{version}.jar from [here](http://central.maven.org/maven2/com/julianpeeters/avrohugger-tools_2.11/0.15.0/avrohugger-tools_2.11-0.15.0.jar) and exec:
> java -jar avrohugger-tools_2.11-0.15.0-assembly.jar generate-specific datafile ./src/main/scala/it/teamDigitale/avro/DataPoint.avsc ./src/main/scala/
#### 2. Create a jar file with external dependencies
> sbt clean compile package universal:package-bin

## Run the code
#### 1. Run a kafka producer
[Here](https://github.com/fabiana001/iotIngestion/tree/master/src/main/scala/it/teamDigitale/kafkaProducers/main) we have implemented some example
of a kafka producer which extract data from an rss feed.

The following command generate a kafka producer which extracts data from [http://opendata.5t.torino.it/get_fdt](http://opendata.5t.torino.it/get_fdt):
> java -Dconfig.file=./application.conf -cp "iotingestion_2.11-1.0.jar:./iotingestion-1.0/lib/*" it.teamDigitale.kafkaProducers.main.TorinoTrafficProducer

#### 2. Run a kafka consumer
[Here](https://github.com/fabiana001/iotIngestion/tree/master/src/main/scala/it/teamDigitale/consumers/sparkStreaming/main) we have implemented some kafka consumer 
which stores data on HDFS and InfluxDB.

For running a kafka consumer (e.g. HDFSConsumerMain):
> java -Dconfig.file=./application.conf -cp "iotingestion_2.11-1.0.jar:./iotingestion-1.0/lib/*" it.teamDigitale.consumers.sparkStreaming.main.HDFSConsumerMain



<!--## Time series databases

### InfluxDB
From [Docker](https://docs.influxdata.com/influxdb/v1.2/)

> $ docker run -d --volume=/var/influxdb:/data -p 8083:8083 -p 8086:8086  influxdb
 -->
