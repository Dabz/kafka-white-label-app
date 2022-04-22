package io.confluent.example.deserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Avro deserializer with error wrapper
 */
public class AvroErrorWrapperDeserializer extends ErrorWrapperDeserializer {
    public AvroErrorWrapperDeserializer() {
        super(new KafkaAvroDeserializer());
    }
}
