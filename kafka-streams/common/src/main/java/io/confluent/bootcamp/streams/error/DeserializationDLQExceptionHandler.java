package io.confluent.bootcamp.streams.error;

import io.confluent.bootcamp.streams.Context;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public class DeserializationDLQExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(DeserializationExceptionHandler.class.getName());

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {
        try {
            var sw = new StringWriter();
            var pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            var stackStrace = sw.toString();

            logger.error("Sending message to the DLQ due to Deserialization issue." +
                            "Input topic {}, partition: {}, offset: {}.\nException: {}\nStacktrace: {}",
                    processorContext.topic(),
                    processorContext.partition(),
                    processorContext.offset(),
                    e,
                    stackStrace);

            Context.sendMessageToDLQ(Context.getDLQTopic(), e, consumerRecord.value(), processorContext.topic(), processorContext.partition(), processorContext.offset());
        } catch (Exception ex) {
            logger.error("Unexpected issue in the deserialization exception handler", ex);
            return DeserializationHandlerResponse.FAIL;
        }

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
