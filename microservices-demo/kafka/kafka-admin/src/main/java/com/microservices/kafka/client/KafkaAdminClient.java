package com.microservices.kafka.client;

import com.microservices.config.KafkaConfigData;
import com.microservices.config.RetryConfigData;
import com.microservices.kafka.config.WebConfig;
import com.microservices.kafka.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);

    @Autowired
    private KafkaConfigData kafkaConfigData;

    @Autowired
    private RetryConfigData retryConfigData;

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private RetryTemplate retryTemplate;

    @Autowired
    private WebClient webClient;

    public void createTopics() throws KafkaClientException {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max amount of retry while creating topics");
        }
        checkTopicsCreated();
    }

    public void checkSchemaRegistry() throws KafkaClientException {
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts().intValue();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs().longValue();

        while (!checkSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus checkSchemaRegistryStatus() {
        try {
           return webClient.method(HttpMethod.GET)
                   .uri(kafkaConfigData.getSchemaRRegistryUrl())
                   .exchange()
                   .map(ClientResponse::statusCode)
                   .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    private void checkTopicsCreated() throws KafkaClientException {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts().intValue();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs().longValue();

        for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    private void sleep(Long sleepTimeMs) throws KafkaClientException {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error while sleeping, fetching new topic");
        }
    }

    private void checkMaxRetry(int retry, Integer maxRetry) throws KafkaClientException {
        if (retry > maxRetry) {
            throw new KafkaClientException("Reached max number of retry");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        if (topics == null) {
            return false;
        }
        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicsName = kafkaConfigData.getTopicNamesToCreate();
        logger.info("Topics size {} and maxAttempts are {} for the following topics {}", topicsName.size(),
                retryConfigData.getMaxAttempts(), topicsName);

        List<NewTopic> kafkaTopic = topicsName.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());

        return adminClient.createTopics(kafkaTopic);
    }

    private Collection<TopicListing> getTopics() throws KafkaClientException {
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (RuntimeException t) {
            throw new KafkaClientException("Error in getting topics", t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws KafkaClientException {
        logger.info("Reading kafka topic {}, attempt {}",
                kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topicListings;
        try {
            topicListings = adminClient.listTopics().listings().get();
            if (topicListings != null) {
                topicListings.forEach(topic -> logger.debug("Topic fetched - {}", topic.name()));
            }
        } catch (Exception e) {
            throw new KafkaClientException("Error while getting topic", e);
        }

        return topicListings;
    }
}
