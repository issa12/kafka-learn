package com.isa;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private final static Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.debug("Kafka Consumer");
        String groupId = "my-java-app";
        String topic = "demo_java";
        // create producer properties
        Properties prop = new Properties();
        //prop.setProperty("bootstrap.servers", "127.0.0.1:9092");

        /*
        https://issa.conduktor.app/admin/my-playground
        user: w22YdpPqqt9urXwJVjgvZ
        security.protocol=SASL_SSL
        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="w22YdpPqqt9urXwJVjgvZ" password="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ3MjJZZHBQcXF0OXVyWHdKVmpndloiLCJvcmdhbml6YXRpb25JZCI6NzY2ODksInVzZXJJZCI6ODkyMjYsImZvckV4cGlyYXRpb25DaGVjayI6IjZhNGU5ZWE5LWNjNmYtNDE5NC1iMGJiLTNlNDg1OGQ0N2VkYiJ9fQ.4ZU_0RDtYCKPfEfeQORkdmdStRdXYzceWJGLIN9yTbA";
        sasl.mechanism=PLAIN
         */

        //prop.setProperty("bootstrap.servers","127.0.0.1:9092");
        prop.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        prop.setProperty("security.protocol", "SASL_SSL");
        prop.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"w22YdpPqqt9urXwJVjgvZ\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ3MjJZZHBQcXF0OXVyWHdKVmpndloiLCJvcmdhbml6YXRpb25JZCI6NzY2ODksInVzZXJJZCI6ODkyMjYsImZvckV4cGlyYXRpb25DaGVjayI6IjZhNGU5ZWE5LWNjNmYtNDE5NC1iMGJiLTNlNDg1OGQ0N2VkYiJ9fQ.4ZU_0RDtYCKPfEfeQORkdmdStRdXYzceWJGLIN9yTbA\";");
        prop.setProperty("sasl.mechanism", "PLAIN");

        // Create a consumer
        prop.setProperty("key.deserializer", StringDeserializer.class.getName());
        prop.setProperty("value.deserializer", StringDeserializer.class.getName());
        prop.setProperty("group.id", groupId);
        prop.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            log.info("================================ Polling ============================");
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> rec : records ) {
                log.info("key: {}, value: {}\nPartition:{}, offset: {}",
                        rec.key(), rec.value(), rec.partition(), rec.offset());
            }


        }



    }
}
