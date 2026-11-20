package br.com.portfolio.bank.controller;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.portfolio.bank.model.TransactionEvent;
import br.com.portfolio.bank.service.TransactionProducerService;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final TransactionProducerService producerService;

    public TransactionController(TransactionProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<TransactionEvent> createTransaction(@RequestBody TransactionRequest request) {
        TransactionEvent event = new TransactionEvent(
                UUID.randomUUID().toString().substring(0, 8),
                request.accountId(),
                request.type(),
                request.amount(),
                Instant.now()
        );
        producerService.sendTransaction(event);
        return ResponseEntity.accepted().body(event);
    }

    public record TransactionRequest(String accountId, String type, BigDecimal amount) {}
}