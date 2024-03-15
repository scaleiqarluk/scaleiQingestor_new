package com.scaleiq.ingestor;//Ingestor EventHandler for Streaming Data, for testing only.
//Note: The versions used are not the latest one, but the compatibles with the Demo.
//Please test with customer compatibles libraries, do not use this library in Production!

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestorEventHandler implements EventHandler {
    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(IngestorEventHandler.class.getSimpleName());

    public IngestorEventHandler(KafkaProducer<String, String> KafkaProducer, String topic) {
        this.kafkaProducer = KafkaProducer;
        this.topic = topic;

    }

    @Override
    public void onOpen() throws Exception {
        //No additional code required
    }

    @Override
    public void onClosed() throws Exception {
        // Close the StreamDemoGenerator
        kafkaProducer.close();

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        //Send code Async
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {
        //No code required here
    }

    @Override
    public void onError(Throwable t) {
        //Errors on Streaming
        log.error("Error detected on the Ingestor Streaming Source", t);
    }
}
