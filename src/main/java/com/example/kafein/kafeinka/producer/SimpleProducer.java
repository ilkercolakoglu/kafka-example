package com.example.kafein.kafeinka.producer;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {
    private static Scanner in;

    public static void main(String[] args) throws Exception {

        String topicName = "kafeinkafka";
        in = new Scanner(System.in);
        System.out.println("Please type your new year messages(For exiting -1)");

        // Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer<String, String>(configProperties);

        String line = in.nextLine();
        while (!line.equals("-1")) {
            ProducerRecord<String, String> rec = new ProducerRecord<>(
                    topicName, line);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }
}