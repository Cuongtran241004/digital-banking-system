package com.banking.account_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {
    private final AccountService accountService;

    /**
     * Consume transaction.completed event from kafka
     * Credits receiver account
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload
    ){
        try {
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("AccountEventConsumer:consumeTransactionCompleted: Crediting receiver account: {} with amount: {}", receiverAccount, amount);

            accountService.creditBalance(receiverAccount, amount);
        } catch (Exception e) {
            log.error("AccountEventConsumer:consumeTransactionCompleted: Error: {}", e.getMessage());
        }
    }



    /**
     * Consume fraud.detected event from kafka
     * Blocks sender account
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public  void consumeFrauDetected(
            @Payload Map<String, Object> payload
    ){
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            //BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("AccountEventConsumer:consumeFrauDetected: Blocking sender account: {}", senderAccount);

            accountService.blockAccount(senderAccount);
            //accountService.creditBalance(senderAccount, amount);


        }catch (Exception e){
            log.error("AccountEventConsumer:consumeFrauDetected: Error: {}", e.getMessage());
        }
    }
}
