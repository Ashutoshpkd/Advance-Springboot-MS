package com.microservices.kafka.producer.config.service;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.Serializable;

public interface KafkaProducer <K extends Serializable, V extends SpecificRecordBase> {
    public void send(String topicName, K key, V message);
}
