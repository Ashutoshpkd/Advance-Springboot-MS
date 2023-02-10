package com.microservices.kafka.client;

import com.microservices.config.KafkaConfigData;
import com.microservices.config.RetryConfigData;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

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
}
