package io.confluent.example;

import io.confluent.dabz.examples.producer.Payment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumerServiceTest {

    private MockConsumer<String, Payment> mockConsumer;
    private ConsumerService consumerService;

    @BeforeEach
    void setUp() {
        // Creating a mock consumer to simulate upcoming messages
        mockConsumer = new MockConsumer<String, Payment>(OffsetResetStrategy.EARLIEST);
        consumerService = new ConsumerService(mockConsumer);

        // Assigning each required topic/partition to our mock consumer
        mockConsumer.assign(Collections.singleton(new TopicPartition(ConsumerService.PAYMENT_TOPIC, 0)));
        // And setting the beginning of each assigned partition to offset 0
        mockConsumer.updateBeginningOffsets(mockConsumer.assignment().stream().collect(Collectors.toMap((entry) -> entry, (entry) -> 0L)));
    }

    /**
     * Simple test to ensure that all messages have been properly consumed
     * Your tests should be more "business" oriented in a real application
     */
    @Test
    void fetchData() throws IOException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Thread thread = new Thread(() -> consumerService.fetchData((record) -> {
            counter.incrementAndGet();
        }));
        thread.start();

        ConsumerRecord<String, Payment> record = new ConsumerRecord<>(ConsumerService.PAYMENT_TOPIC, 0, 1, "Hello World",
                Payment.newBuilder()
                        .setAmount(1)
                        .setId("payment 1.0")
                        .build());
        mockConsumer.addRecord(record);
        record = new ConsumerRecord<>(ConsumerService.PAYMENT_TOPIC, 0, 2, "Hello World",
                Payment.newBuilder()
                        .setAmount(1)
                        .setId("payment 1.0")
                        .build());
        mockConsumer.addRecord(record);

        // As it is done by another thread, let's wait a bit if not all messages have been processed
        if (counter.get() != 2) {
            Thread.sleep(100);
        }

        consumerService.close();
        thread.join();

        assertEquals(2, counter.get());
    }
}