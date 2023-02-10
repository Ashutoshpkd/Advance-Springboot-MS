package com.microservices.common.config;

import com.microservices.config.RetryConfigData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class RetryConfig {

    @Autowired
    private RetryConfigData retryConfigData;

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        ExponentialBackOff ebo = new ExponentialBackOff();
        ebo.setInitialInterval(retryConfigData.getInitialIntervalMs());
        ebo.setMaxInterval(retryConfigData.getMaxIntervalMs());
        ebo.setMultiplier(retryConfigData.getMultiplier());

        retryTemplate.setBackOffPolicy((BackOffPolicy) ebo);

        SimpleRetryPolicy srp = new SimpleRetryPolicy();
        srp.setMaxAttempts(retryConfigData.getMaxAttempts());

        retryTemplate.setRetryPolicy(srp);

        return retryTemplate;
    }
}
