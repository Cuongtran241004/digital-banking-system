package service;

import client.AccountServiceClient;
import dto.TransactionResponse;
import dto.TransferRequest;
import entity.Transaction;
import entity.TransactionStatus;
import entity.TransactionType;
import event.TransactionCompletedEvent;
import event.TransactionInitiatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import repository.TransactionRepository;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {
    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.delected";


    /**
     * SAGA STEP 1 - Initiate transfer
     * Deducts from sender via feign
     * Saves transaction as PROCESSING
     * Publish event to kafka for fraud check
     * @param request
     * @return
     */
    public TransactionResponse transfer(TransferRequest request) {
        log.info("SAGA START - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // SAGA STEP 1 - Deduct from sender account via feign client
        accountServiceClient.deductBalance(
                request.getSenderAccountNumber(),
                request.getAmount()
        );


        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved with ID: {}", savedTransaction.getId());

        // SAGA STEP 2 - Publish TransactionInitiatedEvent to Kafka for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);

        log.info("SAGA STEP 2 - TransactionInitiatedEvent published {}", savedTransaction.getId());


        return mapToResponse(savedTransaction);
    }


    /**
     *
     * @param transactionId
     * @return
     */
    public TransactionResponse getTransaction(String transactionId) {
        // Implement the logic to retrieve a transaction by its ID
        // This is just a placeholder implementation
        return mapToResponse(transactionRepository
        .findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found")));
    }


    /**
     *
     * @param accountNumber
     * @return
     */
    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        // Implement the logic to retrieve transaction history for a given account number
        // This is just a placeholder implementation
        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {

        log.info("OTP verification for the transaction: {}", transactionId);


        Transaction transaction = transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found"));

        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            // OTP EXPIRED
            log.warn("OTP verification for the transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded!");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            // BLOCK ACCOUNT AND REFUND
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);

            blockAccountAndCompensate(transaction, "Wrong OTP entered - transaction cancelled and block for security!");
            return mapToResponse(transaction);
        }

        // OTP correct- complete transaction
        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }


    public void processCleanResult(String transactionId){
        Transaction transaction = transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found"));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} is not PROCESSING - skipping: {}", transactionId);
            return;
        }

        completeTransaction(transaction);
    }
    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - Refunding: {} amount: {}",
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        // CREDIT MONEY BACK TO SENDER SYNCHRONOUSLY
        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount());


        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setFailureReason(reason + " - SAGA Compensation executed, amount refunded at " + LocalDateTime.now());
        transactionRepository.save(transaction);


        // PUBLISH refund event - Notification service will alert sender user
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(),refundEvent);

        log.info("SAGA COMPENSATION - {} refunding to {}", transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {
        Map<String, Object> fraudEvent =  new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked, kindly contact to the bank!",
                transaction.getSenderAccountNumber());


        // SAGA COMPENSATION - Refund Sender
        compensateTransaction(transaction, reason);
    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());

        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(),completedEvent);

        log.info("SAGA COMPENSATION - Transaction {} completed", transaction.getId());
    }



    private TransactionResponse mapToResponse(Transaction transaction) {
        TransactionResponse response = new TransactionResponse();
        response.setId(transaction.getId());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setType(transaction.getType());
        response.setStatus(transaction.getStatus());
        response.setDescription(transaction.getDescription());
        response.setReferenceNumber(transaction.getReferenceNumber());
        return response;
    }
}
