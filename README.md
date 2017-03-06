# iotIngestion
POC for Iot ingestion using Camel, Avro, Kafka and Spark Streaming

## Transform all data into an internal format (Event class) 
To be as efficient as possible in our implementation, we use Apache Avro to serialize/deserialize data from and to Kafka. 
In particular, inspired by [Osso-project](), we defined a common schema for any kind of input events.
This schema is general enough to fit most usage data types (aka event logs, data from mobile devices, sensors, etc.).

To generate a java class from an avro file, download avro-tools-{version}.jar from [here](http://central.maven.org/maven2/com/julianpeeters/avrohugger-tools_2.11/0.15.0/avrohugger-tools_2.11-0.15.0.jar) and exec:
> java -jar avrohugger-tools_2.11-0.15.0-assembly.jar generate-specific datafile ./path/to/Event.avsc ./src/main/scala/

To run the kafka producer:
> java -Dconfig.file=./application.conf -cp "iotingestion_2.11-1.0.jar:./iotingestion-1.0/lib/*" it.teamDigitale.kafkaProducers.ProducerMain

## Time series databases

### InfluxDB
From [Docker](https://docs.influxdata.com/influxdb/v1.2/)

> $ docker run -d --volume=/var/influxdb:/data -p 8083:8083 -p 8086:8086  influxdb
