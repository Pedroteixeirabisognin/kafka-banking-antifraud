package br.com.portfolio.bank.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import br.com.portfolio.bank.config.KafkaTopicConfig;
import br.com.portfolio.bank.model.TransactionEvent;

@Service
public class TransactionProducerService {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducerService.class);
    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    public TransactionProducerService(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransaction(TransactionEvent event) {
        // accountId como chave garante ordenação estrita por conta dentro da partição
        kafkaTemplate.send(KafkaTopicConfig.TRANSACTIONS_TOPIC, event.accountId(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("[ENVIADO] Conta: {} | Partição: {} | Offset: {} | Valor: R$ {}",
                                event.accountId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                event.amount());
                    } else {
                        log.error("Falha ao enviar transação {}: {}", event.txId(), ex.getMessage());
                    }
                });
    }
}