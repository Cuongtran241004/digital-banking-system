package com.banking.fraud_detection_service.service;

import com.banking.fraud_detection_service.client.AccountServiceClient;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import model.FraudCheckResult;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transactions-per-minute}")
    private int maxTransactionsPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    public void checkTransaction(Map<String, Object> payload) {
        // Implement your fraud detection logic here
        log.info("Checking transaction for potential fraud: {}", payload);

        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
        BigDecimal amount = new BigDecimal((String) payload.get("amount"));

        // Fetch real account from account service
        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);

        log.info("Checking transaction {}: sender balance is {}, transaction amount is {}", transactionId, senderBalance, amount);

        FraudCheckResult result = performFraudCheck(accountNumber, amount, senderBalance);

        if(result.isFraud()){
            log.info("Suspicious transaction detected: {}. Reason: {}", transactionId, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
        }else{
            // Transaction is not suspicious, you can log or handle it accordingly
            log.info("Transaction {} is not suspicious.", transactionId);

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);
        }
    }

    private FraudCheckResult performFraudCheck(
            String accountNumber,
            BigDecimal amount,
            BigDecimal senderBalance) {
        // Pattern 1: Velocity Check
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(true, "Too many transaction in 60 seconds" + " - Velocity limit exceeded");
        }

        // Pattern 2: Amount Check
        if(isAmountSuspicious(accountNumber, amount)){
            return new FraudCheckResult(true, "Unusual transaction amount" + " - exceededs 3x your average");
        }

        // Pattern 3: Balance Check
        if(senderBalance.compareTo(BigDecimal.ZERO) > 0
        && isBalanceCheckFailed(senderBalance, amount)){
            return new FraudCheckResult(true, "Transaction exceed 90% of account balance");
        }
        return new FraudCheckResult(false, null);
    }


    private boolean isVelocityExceeded(String accountNumber){
        String key = "fraud:velocity:" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if(count != null && count == 1){
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        log.info("Velocity check for account {}: count = {}/{}", accountNumber, count, maxTransactionsPerMinute);

        return count != null && count > maxTransactionsPerMinute;
    }

    /**
     * Checks if the transaction amount is suspicious based on the average transaction amount for the account.
     * @param accountNumber
     * @param amount
     * @return
     */
    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount){
        String avgKey = "fraud:avg_amount:" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        if(avgStr == null){
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));

        // Update running average
        BigDecimal newAvg = avgAmount.add(amount).divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
        redisTemplate.opsForValue().set(avgKey, newAvg.toString());

        log.info("Amount check - amount: {} threshold: {} suspicious: {}", amount, threshold, amount.compareTo(threshold) > 0);

        return amount.compareTo(threshold) > 0;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount){
        BigDecimal maxAllowed = senderBalance.multiply(
                BigDecimal.valueOf(maxBalancePercentage)
        );

        log.info("Balance check - senderBalance: {} maxAllowed: {} amount: {} suspicious: {}", senderBalance, maxAllowed, amount, amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;
    }
}
