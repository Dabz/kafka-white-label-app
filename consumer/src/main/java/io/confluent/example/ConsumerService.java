package io.confluent.example;

import io.confluent.dabz.examples.producer.Payment;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class ConsumerService implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);
    protected static final String PAYMENT_TOPIC = "payment";

    /**
     * We rely on {@link Consumer} instead of {@link KafkaConsumer} to inject
     * a {@link org.apache.kafka.clients.consumer.MockConsumer} during unit tests
      */
    private final Consumer<String, Payment> kafkaConsumer;
    private boolean isRunning = true;

    public interface ProcessFunction<K, V> {
        void process(ConsumerRecord<K, V> record);
    }

    /**
     * Always inject Apache Kafka client configuration dynamically
     * e.g. from a Configuration file, from a ConfigMap, from Environment variables, etc..
     * @param configurationPath Input stream containing the properties
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
        return new ConsumerService(lKafkaConsumer);
    }

    protected ConsumerService(Consumer<String, Payment> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> isRunning = false));
    }

    /**
     * Fetching data and invoking the process function for each message
     * It is worth noting that it is not required to rely on Lambda function for the processing
     * In this example, the lambda is exposed to write unit test this method
     * @param processFunction Lamda that will be executed for each message
     */
    public void fetchData(ProcessFunction<String, Payment> processFunction) {
        Date lastProcessedMessageTime = null;
        // Note: by default, you will restart consuming at your previous position (committed offset)
        // If that's not the expected behavior, you can seek to a specific location:
        //   kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
        // If there are no committed offset, the parameter auto.offset.reset will define where to start  (by default "latest")
        while (isRunning) {
            ConsumerRecords<String, Payment> consumerRecords;
            try {
                // Note: by default, poll() could return up to 500 messages (max.poll.records parameters)
                // You MUST be able to process those messages in less than 5 minutes (max.poll.interval.ms), otherwise
                // it would trigger a Rebalance and you could enter a vicious infinite loop
                consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
            } catch (SerializationException exception) {
                // If a message can not be deserialized, the poll() method will throw an exception
                // You probably SHOULD catch and handle invalid messages (poison pill)

                // If not handle, a poison pill will cause all your instances to failed, a restart will
                // not fix the situation as they would all process the faulty message
                logger.error("Not able to deserialize messages", exception);
                // TODO: Handle the poison pill, e.g. by writing to a Dead Letter Queue (DLQ) and skipping it

                // Handling and detecting this kind of exception is not easy, there are two main possibilities:
                // 1. To actually do the deserialization outside the `poll()` method and catch exception
                // 2. Implement a custom wrapper Deserializer that returns a specific value in case of issue

                /*
                 * Check {@link io.confluent.example.deserializer.ErrorWrapperDeserializer} and {@link io.confluent.example.deserializer.AvroErrorWrapperDeserializer}
                 * to have an example of the second solution to handle deserialization exception properly
                 */
                isRunning = false;
                break;
            }
            if (consumerRecords.isEmpty()
                    && lastProcessedMessageTime != null
                    && (new Date().getTime() - lastProcessedMessageTime.getTime()) > 5 * 1000) {
                // Most application should NOT quit automatically and should continue listening
                // But for the sake of this standalone application, this is quite convenient
                logger.info("Didn't receive new messages for 5 seconds. Time to kill myself :-)");
                isRunning = false;
                break;
            }
            for (ConsumerRecord<String, Payment> consumerRecord : consumerRecords) {
                try {
                    // The process of a single record must be quick, if you are interacting with external systems (e.g. WS)
                    // that could have long response time (e.g. a few dozen or hundreds milliseconds), you should consider
                    // relying on asynchronous call
                    //
                    // Asynchronous processing  will also bring complexity; e.g. you would need to commit offset manually,
                    // you need to ensure that you do not overload the distant process, that not everything would be queued in memory, etc...
                    //
                    // I highly encourage to have a look at https://github.com/confluentinc/parallel-consumer/ if you are in this
                    // use case
                    processFunction.process(consumerRecord);
                    lastProcessedMessageTime = new Date();
                } catch (Exception exception) {
                    logger.error("Unexpected exception while processing messages", exception);
                    // TODO: Handle the exception, possible implementations are:
                    //  1. By writing to a Dead Letter Queue (DLQ) and skipping this message
                    //  2. By stopping the application
                    isRunning = false;
                    break;
                }
            }
        }

        // ALWAYS close KafkaConsumer during the shutdown of your application
        kafkaConsumer.close();

        logger.info("Finished reading data, closing down!");
    }

    protected void subscribeToToptic() {
        kafkaConsumer.subscribe(Collections.singleton(PAYMENT_TOPIC));
    }

    public void fetchData() {
        fetchData((this::processRecord));
    }

    private void processRecord(ConsumerRecord<String, Payment> consumerRecord) {
        // Not a very useful processing, but that's an example :-)
        logger.info("received key: {}, value: {}, headers: {}", consumerRecord.key(), consumerRecord.value(), consumerRecord.headers());
    }

    public static void main(String[] args) throws Exception {
        InputStream configStream;
        if (args.length < 1) {
            logger.warn("Kafka configuration file not specified, relying on default one");
            configStream = ConsumerService.class.getResourceAsStream("/kafka.properties");
        } else {
            configStream = new FileInputStream(args[0]);
        }

        ConsumerService consumerService = ConsumerService.buildConsumer(configStream);
        consumerService.subscribeToToptic();
        consumerService.fetchData();
        configStream.close();
    }

    @Override
    public void close() throws IOException {
        isRunning = false;
    }
}
