package io.confluent.bootcamp.streams.error;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ProcessingException<EV> {
    private final int partition;
    private final String topic;
    private final long offset;
    private final EV value;
    private final byte[] valueBytes;
    private final String stackStrace;
    private Throwable exception;

    public ProcessingException(Throwable exception, ProcessorContext context, EV value, Serde<EV> serde) {
        this.partition = context.partition();
        this.topic = context.topic();
        this.offset = context.offset();
        this.exception = exception;
        this.value = value;
        this.valueBytes = value == null ? new byte[0] : serde.serializer().serialize(context.topic(), value);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        this.stackStrace = sw.toString();
    }

    public ProcessingException(Exception e) {
        this.partition = -1;
        this.topic = "unknown";
        this.offset = -1;
        this.exception = e;
        this.value = null;
        this.valueBytes = null;

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        this.stackStrace = sw.toString();
    }

    public void addHeaders(ProcessorContext context) {
        StringSerializer stringSerializer = new StringSerializer();
        LongSerializer longSerializer = new LongSerializer();
        IntegerSerializer integerSerializer = new IntegerSerializer();


        context.headers().add(new RecordHeader("topic", stringSerializer.serialize("", this.topic)));
        context.headers().add(new RecordHeader("partition", integerSerializer.serialize("", this.partition)));
        context.headers().add(new RecordHeader("offset", longSerializer.serialize("", this.offset)));
        context.headers().add(new RecordHeader("exception", stringSerializer.serialize("", this.exception.getMessage())));
        context.headers().add(new RecordHeader("stack", stringSerializer.serialize("", this.stackStrace)));
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public long getOffset() {
        return offset;
    }

    public EV getValue() {
        return value;
    }

    public byte[] getValueBytes() {
        return valueBytes;
    }

    public Throwable getException() {
        return exception;
    }

    public String getStackStrace() {
        return stackStrace;
    }
}
