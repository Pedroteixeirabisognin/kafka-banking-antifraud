package br.com.portfolio.bank.model;

import java.math.BigDecimal;
import java.time.Instant;

public record TransactionEvent(
        String txId,
        String accountId,
        String type,      // PIX, CREDIT_CARD, TED
        BigDecimal amount,
        Instant timestamp
) {}