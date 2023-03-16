package com.microservices.twitter.to.kafka.listner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterToKafkaServiceListener extends StatusAdapter {

    private static final Logger logger = LoggerFactory.getLogger(TwitterToKafkaServiceListener.class);

    @Override
    public void onStatus(Status status) {
        logger.info("Twitter status with text {} ", status.getText());
    }
}
