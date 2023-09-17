package com.isa;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private final static Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.debug("Hello demo");
        // create producer properties
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","127.0.0.1:9092");

        /*
        https://issa.conduktor.app/admin/my-playground
        user: w22YdpPqqt9urXwJVjgvZ
        security.protocol=SASL_SSL
        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="w22YdpPqqt9urXwJVjgvZ" password="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ3MjJZZHBQcXF0OXVyWHdKVmpndloiLCJvcmdhbml6YXRpb25JZCI6NzY2ODksInVzZXJJZCI6ODkyMjYsImZvckV4cGlyYXRpb25DaGVjayI6IjZhNGU5ZWE5LWNjNmYtNDE5NC1iMGJiLTNlNDg1OGQ0N2VkYiJ9fQ.4ZU_0RDtYCKPfEfeQORkdmdStRdXYzceWJGLIN9yTbA";
        sasl.mechanism=PLAIN
         */

        //prop.setProperty("bootstrap.servers","127.0.0.1:9092");
        prop.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        prop.setProperty("security.protocol","SASL_SSL");
        prop.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"w22YdpPqqt9urXwJVjgvZ\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ3MjJZZHBQcXF0OXVyWHdKVmpndloiLCJvcmdhbml6YXRpb25JZCI6NzY2ODksInVzZXJJZCI6ODkyMjYsImZvckV4cGlyYXRpb25DaGVjayI6IjZhNGU5ZWE5LWNjNmYtNDE5NC1iMGJiLTNlNDg1OGQ0N2VkYiJ9fQ.4ZU_0RDtYCKPfEfeQORkdmdStRdXYzceWJGLIN9yTbA\";");
        prop.setProperty("sasl.mechanism","PLAIN");
        prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty("value.serializer",StringSerializer.class.getName());


        // create a producer
        try {
            KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java","test key" ,"value demo java");

            // send data
            producer.send(producerRecord);
            producer.flush();
            producer.close();
        } catch (Exception e) {
            log.error("Error Sending=", e);
        }
    }
}
