# Contexto do Projeto: Motor de Transações Bancárias e Antifraude (Kafka + Spring Boot)

Este arquivo fornece instruções de arquitetura, stack tecnológica e regras de negócio para agentes de IA que auxiliam no desenvolvimento e manutenção deste repositório.

## 1. Visão Geral e Objetivo
Sistema de arquitetura orientada a eventos (*Event-Driven Architecture*) que simula o processamento assíncrono de transações financeiras (PIX, Cartão de Crédito, TED), realizando detecção de fraude em tempo real e registro de extrato/auditoria de forma desacoplada[cite: 1].

## 2. Stack Tecnológica e Versões (CRÍTICO)
* **Linguagem:** Java 21
* **Framework:** Spring Boot **4.2.0** (`spring-boot-starter-webmvc`, `spring-boot-starter-kafka`)
* **Mensageria:** Apache Kafka **4.4.X** rodando em modo **KRaft** (sem ZooKeeper)[cite: 4]
* **Infraestrutura Local:** Docker & Docker Compose (`apache/kafka:latest` e `provectuslabs/kafka-ui:latest`)[cite: 1]
* **Portas Padrão:**
  * API Spring Boot: `8081`
  * Broker Kafka (Externo/Host): `localhost:9092`[cite: 1]
  * Broker Kafka (Interno/Docker): `kafka:29092`[cite: 1]
  * Kafka UI: `http://localhost:8080`[cite: 1]

## 3. Regras de Compatibilidade (Spring Boot 4.2 + Jackson 3)
* **UTILIZAR SEMPRE** as classes baseadas no Jackson 3:
  * Producer `value-serializer`: `org.springframework.kafka.support.serializer.JacksonJsonSerializer`
  * Consumer `value-deserializer`: `org.springframework.kafka.support.serializer.JacksonJsonDeserializer`
* Sempre manter o pacote dos DTOs/Records configurado em `spring.json.trusted.packages`.

## 4. Topologia do Apache Kafka
* **Tópico Principal (`financial-transactions`):**
  * **Partições:** 3 (para escalabilidade horizontal entre instâncias consumidoras)[cite: 1, 3].
  * **Chave de Particionamento (`Key`):** `accountId` (String). **Nunca remover a chave**, pois ela garante que todas as transações de uma mesma conta caiam na mesma partição e mantenham ordenação estrita[cite: 1, 3].
  * **Payload (`Value`):** `TransactionEvent` (`txId`, `accountId`, `type`, `amount`, `timestamp`).
* **Tópico de Alertas (`fraud-alerts`):**
  * **Partições:** 3[cite: 1].
  * **Gatilho:** Publicado pelo `FraudDetectionConsumer` sempre que `amount > 5000.00`[cite: 1].

## 5. Garantia de Consistência e Resiliência (Regras Bancárias)
* **Producer Idempotente:** Manter `acks: all` e `enable.idempotence: true` no `application.yml` para evitar duplicação de transações em retentativas[cite: 4].
* **Commit Manual de Offset:** Manter `enable-auto-commit: false` e `listener.ack-mode: manual_immediate`[cite: 1, 4]. Todo `@KafkaListener` deve receber `Acknowledgment ack` e chamar `ack.acknowledge()` **apenas após** concluir o processamento com sucesso.
* **Consumer Groups Independentes:**[cite: 1, 3]
  * `fraud-detection-group`: Responsável pela regra de antifraude e publicação em `fraud-alerts`[cite: 1].
  * `ledger-audit-group`: Responsável pelo registro paralelo de extrato/auditoria sem bloquear o fluxo antifraude[cite: 1].

## 6. Comandos Úteis
* **Subir infraestrutura:** `docker compose up -d`[cite: 1]
* **Rodar aplicação:** `./mvnw clean spring-boot:run`
* **Derrubar infraestrutura:** `docker compose down`[cite: 1]

## 7. Diretrizes para Novas Implementações
Ao adicionar novas funcionalidades (como Dead Letter Topics, Kafka Streams ou persistência em PostgreSQL):
1. Preserve o `accountId` como chave em todos os novos tópicos derivados para manter a rastreabilidade por conta[cite: 1, 3].
2. Atualize o `docker-compose.yml` mantendo a comunicação interna na rede Docker (`kafka:29092`) e externa via `localhost:9092`[cite: 1].
3. Mantenha o `README.md` e a collection do Postman sincronizados com quaisquer novos endpoints ou tópicos criados.