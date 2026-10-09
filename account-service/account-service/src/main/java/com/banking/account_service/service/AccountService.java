package com.banking.account_service.service;

import com.banking.account_service.dto.AccountResponse;
import com.banking.account_service.dto.CreateAccountRequest;
import com.banking.account_service.entity.Account;
import com.banking.account_service.entity.AccountStatus;
import com.banking.account_service.entity.AccountType;
import com.banking.account_service.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {
    private final AccountRepository accountRepository;

    // SecureRandom is used to generate a unique 12-digit account number
    private static final SecureRandom secureRandom = new SecureRandom();

    /**
     * Create a new account
     * @param request
     * @return
     */
    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("AccountService:createAccount: {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("Account already exists!" + request.getEmail());
        }

        Account account = new Account();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(AccountType.valueOf(request.getAccountType()));
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(BigDecimal.valueOf(request.getInitialDeposit()));
        account.setAccountNumber(generateAccountNumber());
        account.setDailyTransactionLimit(
                request.getAccountType() == AccountType.SAVINGS.name() ? BigDecimal.valueOf(100000) : BigDecimal.valueOf(500000)
        );

        Account savedAccount = accountRepository.save(account);
        log.info("AccountService:createAccount: Account created successfully: {}", savedAccount.getAccountNumber());

        return mapToResponse(savedAccount);
    }

    /**
     * Get account details by account number
     * @param accountNumber
     * @return
     */
    public AccountResponse getAccount(String accountNumber) {
        log.info("AccountService:getAccount: {}", accountNumber);
        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found: " + accountNumber));
        return mapToResponse(account);
    }


    /**
     * Get account balance
     * @param accountNumber
     * @return
     */
    public BigDecimal getBalance(String accountNumber) {
        log.info("AccountService:getBalance: {}", accountNumber);
        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found: " + accountNumber));
        return account.getBalance();
    }

    /**
     * Block account - callded by Fraud detection service when fraud is detected. This is a SAGA step 3.
     * @param accountNumber
     * @return
     */
    public void blockAccount(String accountNumber) {
        log.info("AccountService:blockAccount: {}", accountNumber);

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found: " + accountNumber));
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

        log.info("AccountService:blockAccount: Account blocked successfully: {}", accountNumber);
    }

    /**
     * Deduct balance from account - called by Transaction service when transfer is initiated. This is a SAGA step 1.
     * @param accountNumber
     * @param amount
     * @return
     */
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("AccountService:deductBalance: {} amount: {}", accountNumber, amount);

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found: " + accountNumber));

        if(account.getStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account is not active: " + accountNumber);
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient balance in account: " + accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);

        log.info("AccountService:deductBalance: Balance deducted successfully from account: {}", accountNumber);
    }



    /**
     * Credit balance to account - called by Transaction service when transfer is completed or fraud is detected. This is a SAGA step 4.
     * @param accountNumber
     * @param amount
     * @return
     */
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("AccountService:creditBalance: {} amount: {}", accountNumber, amount);

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found: " + accountNumber));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);

        log.info("AccountService:creditBalance: Balance credited successfully to account: {}", accountNumber);
    }



    // Generate a unique 12-digit account number
    private String generateAccountNumber() {
        String accountNumber;

        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);

        }while (accountRepository.existsByAccountnumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account) {
        AccountResponse response = new AccountResponse();
        response.setId(account.getId());
        response.setAccountNumber(account.getAccountNumber());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setStatus(account.getStatus());
        response.setBalance(account.getBalance());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setCreatedAt(account.getCreatedAt());
        response.setUpdatedAt(account.getUpdatedAt());
        return response;
    }
}
