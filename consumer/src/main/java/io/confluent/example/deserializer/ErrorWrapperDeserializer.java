package io.confluent.example.deserializer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Deserializer wrapping another deserializer for error management
 * Each message is returned as a {@link DeserializerResult}
 * This class can be used as is or can be override for convenient to avoid having to specify the inner deserializer
 * @param <T> Expected data type returned by the deserializer
 */
public class ErrorWrapperDeserializer<T> implements Deserializer<ErrorWrapperDeserializer.DeserializerResult> {
    /**
     * Either returns the deserialized message or flag the message as invalid
     */
    public class DeserializerResult<T> {
        private Exception exception = null;
        private T deserializedValue = null;

        public Exception getException() {
            return exception;
        }

        public T getDeserializedValue() {
            return deserializedValue;
        }

        public Boolean valid() {
            return exception == null;
        }

        public DeserializerResult(T deserializedValue) {
            this.deserializedValue = deserializedValue;
        }

        public DeserializerResult(Exception exception) {
            this.exception = exception;
        }
    }
    private final Deserializer<T> innerDeserializer;

    public ErrorWrapperDeserializer(Deserializer<T> innerDeserializer) {
        this.innerDeserializer = innerDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        innerDeserializer.configure(configs, isKey);
    }

    @Override
    public DeserializerResult<T> deserialize(String topic, byte[] data) {
        try {
            return new DeserializerResult<>(innerDeserializer.deserialize(topic, data));
        } catch (Exception e) {
            return new DeserializerResult<>(e);
        }
    }

    @Override
    public DeserializerResult<T> deserialize(String topic, Headers headers, byte[] data) {
        try {
            return new DeserializerResult<>(innerDeserializer.deserialize(topic, headers, data));
        } catch (Exception e) {
            return new DeserializerResult<>(e);
        }
    }

    @Override
    public void close() {
        innerDeserializer.close();
    }
}
