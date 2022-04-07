package io.confluent.example;

import io.confluent.dabz.examples.producer.Payment;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);
    private final String PAYMENT_TOPIC = "payment";

    /**
     * We rely on {@link Consumer} instead of {@link KafkaConsumer} to inject
     * a {@link org.apache.kafka.clients.consumer.MockConsumer} during unit tests
      */
    private Consumer<String, Payment> kafkaConsumer;
    private boolean isRunning = true;

    /**
     * Always inject Apache Kafka client configuration dynamically
     * e.g. from a Configuration file, from a ConfigMap, from Environment variables, etc..
     * @param configurationPath path of the configuration file containing the client configuration
     */
    public static ConsumerService buildConsumer(InputStream configurationPath) throws Exception {
        var properties = new Properties();
        properties.load(configurationPath);

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        var kafkaConsumer = new KafkaConsumer<String, Payment>(properties);

        return buildConsumer(kafkaConsumer);
    }

    public static ConsumerService buildConsumer(Consumer<String, Payment> lKafkaConsumer) {
        ConsumerService consumerService = new ConsumerService(lKafkaConsumer);
        return consumerService;
    }

    private ConsumerService(Consumer<String, Payment> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
        }));
    }

    public void fetchData() {
        kafkaConsumer.subscribe(Collections.singleton(PAYMENT_TOPIC));
        // Note: by default, you will restart consuming at the latest position (committed offset)
        // If that's not the expected behavior, you can seek to a specific location:
        //   kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
        while (isRunning) {
            ConsumerRecords<String, Payment> consumerRecords = null;
            try {
                consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
            } catch (SerializationException exception) {
                // If a message can not be deserialized, the poll() method will throw an exception
                // You probably SHOULD catch and handle invalid messages (poison pill)

                // If not handle, a poison pill will cause all your instances to failed, a restart will
                // not fix the situation as they would all process the faulty message
                logger.error("Not able to deserialize messages", exception);
                // TODO: Handle the poison pill, e.g. by writing to a Dead Letter Queue (DLQ) and skipping it
                isRunning = false;
                break;
            }
            if (consumerRecords.isEmpty()) {
                // Most application should NOT quit automatically and should continue listening
                logger.info("Didn't receive new messages for 5 seconds. Time to kill myself :-)");
                isRunning = false;
                break;
            }
            for (ConsumerRecord<String, Payment> consumerRecord : consumerRecords) {
                try {
                    processRecord(consumerRecord);
                } catch (Exception exception) {
                    logger.error("Unexpected exception while processing messages", exception);
                    // TODO: Handle the exception, e.g. by writing to a Dead Letter Queue (DLQ) and skipping it
                    isRunning = false;
                    break;
                }
            }
        }

        // ALWAYS close KafkaConsumer during the shutdown of your application
        kafkaConsumer.close();

        logger.info("Finished reading data, closing down!");
    }

    private void processRecord(ConsumerRecord<String, Payment> consumerRecord) {
        logger.info("received key: {}, value: {}, headers: {}", consumerRecord.key(), consumerRecord.value(), consumerRecord.headers());
    }

    public static void main(String[] args) throws Exception {
        InputStream configStream = null;
        if (args.length < 1) {
            logger.warn("Kafka configuration file not specified, relying on default one");
            configStream = ConsumerService.class.getResourceAsStream("/kafka.properties");
        } else {
            configStream = new FileInputStream(args[0]);
        }

        ConsumerService consumerService = ConsumerService.buildConsumer(configStream);
        consumerService.fetchData();
        configStream.close();
    }
}
