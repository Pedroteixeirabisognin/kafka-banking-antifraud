package br.com.portfolio.bank.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import br.com.portfolio.bank.config.KafkaTopicConfig;
import br.com.portfolio.bank.model.TransactionEvent;

@Service
public class LedgerAuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(LedgerAuditConsumer.class);

    @KafkaListener(topics = KafkaTopicConfig.TRANSACTIONS_TOPIC, groupId = "ledger-audit-group")
    public void recordLedger(ConsumerRecord<String, TransactionEvent> record, Acknowledgment ack) {
        TransactionEvent event = record.value();
        log.info("📒 [EXTRATO] Partição: {} | Offset: {} | Conta: {} | Tipo: {} | Valor: R$ {}",
                record.partition(), record.offset(), event.accountId(), event.type(), event.amount());
        ack.acknowledge();
    }
}