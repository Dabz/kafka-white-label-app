package io.confluent.example;

import io.confluent.dabz.examples.producer.Payment;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class ProducerServiceTest {

    private MockProducer<String, Payment> mockProducer;
    private ProducerService producerService;
    private MockSchemaRegistryClient mockSchemaRegistryClient;

    @BeforeEach
    void setUp() {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(mockSchemaRegistryClient);
        kafkaAvroSerializer.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://local"), false);
        mockProducer = new MockProducer(true,
                new StringSerializer(),
                kafkaAvroSerializer);
        producerService = new ProducerService(mockProducer);
    }

    @Test
    void ensureMessageGotProducer() {
        producerService.generateDataInKafka();
        List<ProducerRecord<String, Payment>> recordsSent = mockProducer.history();
        Assertions.assertEquals(100, recordsSent.size());
    }
}