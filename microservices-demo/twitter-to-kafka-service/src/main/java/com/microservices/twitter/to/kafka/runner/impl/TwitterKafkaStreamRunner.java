package com.microservices.twitter.to.kafka.runner.impl;

import com.microservices.config.TwitterToKafkaServiceConfigData;
import com.microservices.twitter.to.kafka.listner.TwitterToKafkaServiceListner;
import com.microservices.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnExpression("not ${twitter-to-kafka-service.enable-mock-tweets} && not ${twitter-to-kafka-service.enable-v2-tweets}")
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    @Autowired
    private TwitterToKafkaServiceConfigData twitterToKafkaConfigData;

    @Autowired
    private TwitterToKafkaServiceListner twitterToKafkaListner;

    private TwitterStream twitterStream;

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterToKafkaListner);
        addFilter();
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);

        logger.info("Started filtering the twitter streams with keywords - {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public void shutDown() {
        if (twitterStream != null) {
            logger.info("Closing twitter stream!");
            twitterStream.shutdown();
        }
    }
}
