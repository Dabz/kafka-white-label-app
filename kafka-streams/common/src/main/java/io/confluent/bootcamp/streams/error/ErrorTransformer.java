package io.confluent.bootcamp.streams.error;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorTransformer<K, V> implements Transformer<K, ProcessingResult<V>, KeyValue<Void, byte[]>> {
    private ProcessorContext localContext;
    private static Logger logger = LoggerFactory.getLogger(ErrorTransformer.class.getName());

    @Override
    public void init(ProcessorContext context) {
        localContext = context;
    }

    @Override
    public KeyValue<Void, byte[]> transform(K key, ProcessingResult<V> value) {
        logger.error("Sending message to the DLQ. Input topic {}, partition: {}, offset: {}.\nException: {}\nStacktrace: {}",
                value.getException().getTopic(),
                value.getException().getPartition(),
                value.getException().getOffset(),
                value.getException().getException(),
                value.getException().getStackStrace());
        value.getException().addHeaders(localContext);
        return new KeyValue<>(null, value.getException().getValueBytes());
    }

    @Override
    public void close() {

    }
}
