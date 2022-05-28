package com.microservice.demo.twitter.to.kafka.service.runner.impl;


import com.microservice.demo.config.TwitterToKafkaServiceConfigData;
import com.microservice.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservice.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservice.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();

    private static  final String[] WORDS = new String[]{
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetur",
            "adipiscing",
            "elit",
            "nostrud",
            "dolore",
            "magna",
            "massa",
            "voluptate"
    };
    private static final String tweetAsRawJson = "{"+
        "\"created_at\":\"{0}\"," +
        "\"id\":\"{1}\"," +
        "\"text\":\"{2}\"," +
        "\"user\":{\"id\":\"{3}\"}" +
        "}";

    private static final String TWITTER_STATUS_DATE_FORMAT= "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keyWords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetsLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetsLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Starting mock filtering twitter stream for keywords {}", Arrays.toString(keyWords));
        simulateTwitterStream(keyWords, minTweetsLength, maxTweetsLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keyWords, int minTweetsLength, int maxTweetsLength, long sleepTimeMs){
        Executors.newSingleThreadExecutor().submit(() ->{
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keyWords, minTweetsLength, maxTweetsLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            }catch (TwitterException e){
                LOG.error("Error creating twitter status!", e);
            }
        });


    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        }catch (InterruptedException e) {
           throw new TwitterToKafkaServiceException("error while sleeping for waiting new status to create");
        }
    }

    private String getFormattedTweet(String[] keyWords, int minTweetsLength, int maxTweetsLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRamdomTweetContent(keyWords,minTweetsLength,maxTweetsLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;

        for (int i = 0; i < params.length; i++){
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRamdomTweetContent(String[] keyWords, int minTweetsLength, int maxTweetsLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetsLength - minTweetsLength + 1) + minTweetsLength;
        return constructRamdomTweets(keyWords, tweet, tweetLength);
    }

    private String constructRamdomTweets(String[] keyWords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength /2 ) {
                tweet.append(keyWords[RANDOM.nextInt(keyWords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
