# Getting Started

### Reference Documentation
For further reference, please consider the following sections:

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/2.2.2.RELEASE/maven-plugin/)
* [Apache Kafka Streams Support](https://docs.spring.io/spring-kafka/docs/current/reference/html/_reference.html#kafka-streams)
* [Apache Kafka Streams Binding Capabilities of Spring Cloud Stream](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/#_kafka_streams_binding_capabilities_of_spring_cloud_stream)
* [Spring for Apache Kafka](https://docs.spring.io/spring-boot/docs/2.2.2.RELEASE/reference/htmlsingle/#boot-features-kafka)
* [Spring Boot Admin (Server)](https://codecentric.github.io/spring-boot-admin/current/#getting-started)

### Guides
The following guides illustrate how to use some features concretely:

* [Samples for using Apache Kafka Streams with Spring Cloud stream](https://github.com/spring-cloud/spring-cloud-stream-samples/tree/master/kafka-streams-samples)

Please follow these steps:

Firstly, you have to download Apache Kafka from official site. https://kafka.apache.org/downloads

Please attention! You've to download binary version of it.

If your OS is windows, change your server.properties file in config directory.

* find logs.dir and change it with log.dir=C:\app\kafka_2.11-2.4.0\kafka-logs
Attention!!! Of course the path can be changed your kafka version

* After your kafka on local is ready. You should start zookeeper,kafka server respectively.

* You should open Powershell. You can reach it Shift+Right click on kafka directory.

Please open 4 different powershell terminal.

* First of all we have to launch zookeeper.
* .\bin\windows\zookeeper-server-start.bat config\zookeeper.properties

* Secondly, we have to launch kafka server.
* .\bin\windows\kafka-server-start.bat .\config\server.properties

* And then we can create a producer for sending data to kafka server
* .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic ilkerkafka

* And we can create a consumer for getting data from kafka server
* .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic kafeinkafka --from-beginning


Also, This application provides us a producer. 
You can write message from console and if you create a consumer,
 you can listen from the topic the application created.
