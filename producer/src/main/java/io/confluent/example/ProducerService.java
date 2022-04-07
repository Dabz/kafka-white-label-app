package io.confluent.example;

import io.confluent.dabz.examples.producer.Payment;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class ProducerService {
    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private final String PAYMENT_TOPIC = "payment";
    private final String ORIGIN_HEADER = "origin";

    /**
     * We rely on {@link Producer} instead of {@link KafkaProducer} to inject
     * a {@link org.apache.kafka.clients.producer.MockProducer} during unit tests
      */
    private Producer<String, Payment> kafkaProducer;

    /**
     * Always inject Apache Kafka client configuration dynamically
     * e.g. from a Configuration file, from a ConfigMap, from Environment variables, etc..
     * @param configurationPath path of the configuration file containing the client configuration
     */
    public static ProducerService buildProducer(InputStream configurationPath) throws Exception {
        var properties = new Properties();
        properties.load(configurationPath);

        // NOTE: For the key serializer, ONLY relies on primitive type (String, Integer, Double, etc...)
        // The ordering of messages is defined by their keys, complex types, like JSON, Avro, Protobuf, could
        // result in different bytes[] even if they are semantically identical (e.g. { "a": 1, "b": 2 } might be
        // serialized as '{ "a": 1, "b": 2 }' or '{ "b": 2, "a": 1 }'
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // NOTE: For public topic, always rely on the Schema Registry to track the schema of the data
        // At the time of the writing, Confluent Schema Registry does support Avro, Protocol Buffers and JSONSchema
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        var kafkaProducer = new KafkaProducer<String, Payment>(properties);

        // ALWAYS close KafkaProducer during the shutdown of your application
        // Forgetting to close KafkaProducer might result in data not being sent to the broker
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close));

        return buildProducer(kafkaProducer);
    }

    public static ProducerService buildProducer(Producer<String, Payment> lKafkaProducer) {
        ProducerService producerService = new ProducerService(lKafkaProducer);
        return producerService;
    }

    private ProducerService(Producer<String, Payment> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void generateDataInKafka() {
        for (int i = 0; i < 100; i++) {
            // Builder is ensuring that the payload is valid
            // Quite convenient to avoid surprises during serialization
            Payment payment = Payment.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setAmount(new Random().nextInt(1000))
                    .build();

            var producerRecord = new ProducerRecord<>(PAYMENT_TOPIC, payment.getId().toString(), payment);

            // Technical metadata, such as correlation ID, origin, flow information, etc... should be added
            // as a header and should not be included in the payload of the message
            producerRecord.headers().add(ORIGIN_HEADER, new StringSerializer().serialize(null, "example applications"));

            // NOTE: Send is an asynchronous method, avoid doing synchronous send (kafkaProducer.send(...).get())
            // Synchronous send will affect performance of your applications

            // NOTE: KafkaProducer already have builtin retries builtin
            // With the default configuration, it retries for 2 minutes all "Retriable" exceptions
            kafkaProducer.send(producerRecord, ((metadata, exception) -> {
                // In the callback, always ensure that the operation has been successful by checking if there is
                // an execution
                if (exception != null) {
                    logger.error("Failed sending message to Apache Kafka", exception);
                    return;
                }

                // To avoid verbose logging, rely on TRACE or DEBUG level
                logger.trace("Successfully sent message {} to partition {} and offset {}", payment.getId(), metadata.partition(), metadata.partition());
            }));
        }

        logger.info("Finished sending data, closing down!");
    }

    public static void main(String[] args) throws Exception {
        InputStream configStream = null;
        if (args.length < 1) {
            logger.warn("Kafka configuration file not specified, relying on default one");
            configStream = ProducerService.class.getResourceAsStream("/kafka.properties");
        } else {
            configStream = new FileInputStream(args[0]);
        }
        ProducerService producerService = ProducerService.buildProducer(configStream);
        producerService.generateDataInKafka();
        configStream.close();
    }
}
