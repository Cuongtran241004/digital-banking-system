package com.banking.fraud_detection_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {
    private final FraudDetectionService fraudDetectionService;

    /**
     * Listens to transaction.initiated topic and consumes TransactionInitiatedEvent messages
     * Every transaction initiated event is processed to check for potential fraud
     * @param payload
     */
    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    private void consumeTransactionInitiated(
            @Payload Map<String, Object> payload
            ){
        log.info("Received TransactionInitiatedEvent: {}", payload);

        try{
            fraudDetectionService.checkTransaction(payload);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
