package entity;

/**
 * Transaction Lifecycle Status
 * PENDING -> PROCESSING -> COMPLETED (clean transaction)
 *                       -> PENDING_VERIFICATION (suspicious detected)
 *                              -> COMPLETED (verified)
 *                              -> FLAGGED (SAGA refund)
 *                       -> FAILED  (failed transaction)
 *                       -> FLAGGED (SAGA refund)
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
