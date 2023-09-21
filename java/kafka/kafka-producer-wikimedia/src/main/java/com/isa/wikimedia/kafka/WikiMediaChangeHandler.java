package com.isa.wikimedia.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeHandler implements EventHandler {

    private KafkaProducer<String, String> producer;
    private String topic = "";

    private final Logger log = LoggerFactory.getLogger(WikiMediaChangeHandler.class.getName());

    public WikiMediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        producer.close();

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info("message Data {}", messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        log.error("error", t);

    }
}
