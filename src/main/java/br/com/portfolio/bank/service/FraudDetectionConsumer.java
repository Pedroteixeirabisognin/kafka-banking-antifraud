package br.com.portfolio.bank.service;

import java.math.BigDecimal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import br.com.portfolio.bank.config.KafkaTopicConfig;
import br.com.portfolio.bank.model.TransactionEvent;

@Service
public class FraudDetectionConsumer {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionConsumer.class);
    private static final BigDecimal FRAUD_THRESHOLD = new BigDecimal("5000.00");

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    public FraudDetectionConsumer(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaTopicConfig.TRANSACTIONS_TOPIC, groupId = "fraud-detection-group")
    public void analyzeTransaction(ConsumerRecord<String, TransactionEvent> record, Acknowledgment ack) {
        TransactionEvent event = record.value();

        if (event.amount().compareTo(FRAUD_THRESHOLD) > 0) {
            log.warn("🚨 [ALERTA DE FRAUDE] Partição: {} | Conta: {} | Valor: R$ {} -> Publicando em {}",
                    record.partition(), event.accountId(), event.amount(), KafkaTopicConfig.ALERTS_TOPIC);
            kafkaTemplate.send(KafkaTopicConfig.ALERTS_TOPIC, event.accountId(), event);
        } else {
            log.info("✅ [APROVADA] Partição: {} | Conta: {} | Valor: R$ {}",
                    record.partition(), event.accountId(), event.amount());
        }

        // Confirma o offset manualmente após concluir a análise
        ack.acknowledge();
    }
}