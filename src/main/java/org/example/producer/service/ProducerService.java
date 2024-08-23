package org.example.producer.service;

import org.example.producer.model.Client;
import org.example.producer.model.Transaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
public class ProducerService {

    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper;
    private final String clientTopic;
    private final String transactionTopic;

    public ProducerService(KafkaProducer<String, String> kafkaProducer,
            ObjectMapper objectMapper,
            @Value("${client.topic}") String clientTopic,
            @Value("${transaction.topic}") String transactionTopic) {
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
        this.clientTopic = clientTopic;
        this.transactionTopic = transactionTopic;
    }

    public Future<RecordMetadata> publishClient(Client client) throws JsonProcessingException {
        String clientJson = objectMapper.writeValueAsString(client);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(clientTopic, client.getClientId().toString(), clientJson);
        return kafkaProducer.send(record);
    }

    public Future<RecordMetadata> publishTransaction(Transaction transaction) throws JsonProcessingException {
        String transactionJson = objectMapper.writeValueAsString(transaction);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(transactionTopic, transaction.getClientId().toString(), transactionJson);
        return kafkaProducer.send(record);
    }
}
