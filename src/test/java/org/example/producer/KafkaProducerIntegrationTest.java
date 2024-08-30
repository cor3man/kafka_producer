package org.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.producer.model.Client;
import org.example.producer.model.Transaction;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class KafkaProducerIntegrationTest {

    public static final String HOST = "http://localhost:";

    private static KafkaContainer kafkaContainer;
    private static TestRestTemplate restTemplate;
    private static ObjectMapper objectMapper;
    private static String bootstrapServers;

    @LocalServerPort
    private int port;

    @BeforeAll
    public static void setUp() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
        kafkaContainer.start();
        bootstrapServers = kafkaContainer.getBootstrapServers();

        restTemplate = new TestRestTemplate();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> kafkaContainer.getBootstrapServers());
    }

    @AfterAll
    public static void tearDown() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    @Test
    public void testCreateClient() throws Exception {
        Client client = Client.builder()
                .clientId(1L)
                .email("john.doe@example.com")
                .firstName("John")
                .lastName("Doe")
                .build();

        ResponseEntity<String> clientResponse = restTemplate.postForEntity(HOST + port + "/api/clients",
                client, String.class);

        assertTrue(clientResponse.getStatusCode().is2xxSuccessful());
        assertKafkaMessage("client-topic", client);
    }

    @Test
    public void testCreateTransaction() throws Exception {
        Transaction transaction = Transaction.builder()
                .bank("Bank A")
                .clientId(1L)
                .orderType(Transaction.TransactionType.INCOME)
                .quantity(100)
                .price(50.0)
                .createdAt(LocalDateTime.now())
                .build();


        ResponseEntity<String> transactionResponse = restTemplate.postForEntity(
                HOST + port + "/api/transactions", transaction, String.class);

        assertTrue(transactionResponse.getStatusCode().is2xxSuccessful());
        assertKafkaMessage("transaction-topic", transaction);
    }

    private void assertKafkaMessage(String topic, Object expectedMessage) throws Exception {
        Properties consumerProps = getConsumerProps();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord<String, String> record : records) {
                Object actualMessage = objectMapper.readValue(record.value(), expectedMessage.getClass());

                assertThat(actualMessage).isEqualTo(expectedMessage);
                return;
            }
        }

        throw new AssertionError("Expected message not found in topic: " + topic);
    }

    private static @NotNull Properties getConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProps;
    }
}
