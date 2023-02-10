package com.microservices.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka-service")
public class KafkaConfigData {
    private String bootstrapServer;
    private String schemaRegistryUrlKey;
    private String schemaRRegistryUrl;
    private String topicName;
    private List<String> topicNamesToCreate;
    private Integer numOfPartitions;
    private Short replicationFactor;
}
