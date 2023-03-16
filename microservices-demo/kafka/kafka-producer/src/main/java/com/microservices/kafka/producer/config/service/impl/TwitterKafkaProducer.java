package com.microservices.kafka.producer.config.service.impl;

import com.microservices.kafka.avro.model.TwitterAvroModel;
import com.microservices.kafka.producer.config.service.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    @Autowired
    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    @PreDestroy
    public void destroy() {
        if (kafkaTemplate != null) {
            kafkaTemplate.destroy();
        }
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        logger.info("Sending message to topic = {}, message = {}", topicName, message);
        ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResult =
                kafkaTemplate.send(topicName, key, message);
        kafkaResult.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.error("Error while sending message - {} to topic - {}", message.toString(), topicName);
            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
                RecordMetadata recordMetadata = result.getRecordMetadata();
                logger.debug("Received meta data topic - {}, timestamp - {}", recordMetadata.topic(), recordMetadata.timestamp());
            }
        });
    }
}
