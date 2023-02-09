package com.microservices.twitter.to.kafka;

import com.microservices.config.TwitterToKafkaServiceConfigData;
import com.microservices.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices")
public class TwitterToKafkaApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(TwitterToKafkaApplication.class);

    @Autowired
    private StreamRunner stream;

    @Autowired
    private TwitterToKafkaServiceConfigData twitterToKafkaConfig;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        stream.start();
        logger.info("[PERFLOG] - " + "Twitter to Kafka Variables - " + twitterToKafkaConfig.getTwitterKeywords());
        logger.info("Twitter variables - Base url - " + twitterToKafkaConfig.getTwitterV2BaseUrl());
        logger.info("Twitter variables - Token - " + twitterToKafkaConfig.getTwitterV2BearerToken());
    }
}
