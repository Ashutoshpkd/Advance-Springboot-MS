package com.microservices.twitter.to.kafka.runner.impl;

import com.microservices.config.TwitterToKafkaServiceConfigData;
import com.microservices.twitter.to.kafka.exception.TwitterToKafkaException;
import com.microservices.twitter.to.kafka.listner.TwitterToKafkaServiceListener;
import com.microservices.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    @Autowired
    private TwitterToKafkaServiceConfigData twitterToKafkaConfigData;

    @Autowired
    private TwitterToKafkaServiceListener twitterToKafkaListner;

    private static final String TWITTER_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    private static final String[] WORDS = {
        "Attack on titans",
        "Naruto",
        "Eren Jeager",
        "Attack titan",
        "Demon Slayer",
        "Saitama",
        "One Punch Man",
        "Peace",
        "Gold",
        "Power",
        "Software Developer",
        "Ashutosh",
        "Believe",
        "Blessed",
        "Thankful",
        "Focused",
        "Life"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaConfigData.getTwitterKeywords().toArray(new String[0]);
        Integer minLength = twitterToKafkaConfigData.getMockMinTweetLength();
        int maxLength = twitterToKafkaConfigData.getMockMaxTweetLength();
        long sleepTime = twitterToKafkaConfigData.getMockSleepMs();

        logger.info("Starting the mock twitter stream for keywords, {} ", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minLength, maxLength, sleepTime);
    }

    private void sleep(Long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (Exception e) {
            throw new TwitterToKafkaException("Error while sleeping, waiting for the new status");
        }
    }

    private void simulateTwitterStream(String[] keywords, int  minLength, int maxLength, Long sleepTime)
    throws TwitterException {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweets(keywords, minLength, maxLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterToKafkaListner.onStatus(status);
                    sleep(sleepTime);
                }
            } catch (Exception e) {
                logger.error("Error in creating twitter status, {} ", e.getMessage());
                throw new TwitterToKafkaException("Error in creating status", e);
            }
        });
    }

    private String getFormattedTweets(String[] keywords, int minLength, int maxLength) {
        String[] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong()),
                getRandomTweetContent(keywords, minLength, maxLength),
                String.valueOf(ThreadLocalRandom.current().nextLong()),
        };
        String tweet = tweetAsRawJson;

        for (int i=0; i< params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }

        logger.info("Returned tweet = " + tweet);
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minLength, int maxLength) {
        StringBuilder tweet = new StringBuilder();
        Random random = new Random();
        int randomLen = random.nextInt((maxLength - minLength + 1) + minLength);

        for (int i=0; i<randomLen; i++) {
            tweet.append(WORDS[random.nextInt(WORDS.length)]).append(" ");
            if (i == randomLen/2) {
                tweet.append(keywords[random.nextInt(keywords.length)]).append(" ");
            }
        }

        return tweet.toString().trim();
    }
}
