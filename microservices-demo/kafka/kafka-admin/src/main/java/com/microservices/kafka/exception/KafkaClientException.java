package com.microservices.kafka.exception;

public class KafkaClientException extends Exception {
    public KafkaClientException() {
        super();
    }
    public KafkaClientException(String message) {
        super(message);
    }
    public KafkaClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
