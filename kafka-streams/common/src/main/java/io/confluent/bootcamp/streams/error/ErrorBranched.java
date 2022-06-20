package io.confluent.bootcamp.streams.error;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Produced;

public class ErrorBranched {
    public static <K, V> Branched<K, V> branchedToDLQ(String dlqTopicName) {
        return Branched.withConsumer((kt) -> kt
                .transform(() -> new ErrorTransformer())
                .to(dlqTopicName, Produced.with(Serdes.Void(), Serdes.ByteArray())));

    }
}
