package io.confluent.bootcamp.streams.error;

public class ProcessingResult<V> {
    private V value;
    private ProcessingException exception;

    private ProcessingResult(V lvalue) {
        this.value = lvalue;
    }

    private ProcessingResult(ProcessingException lerror) {
        this.exception = lerror;
    }

    public static <V, EV> ProcessingResult success(V value) {
        return new ProcessingResult(value);
    }

    public static <V> ProcessingResult fail(ProcessingException exception) {
        return new ProcessingResult(exception);
    }

    public boolean isValid() {
        return exception == null && value != null;
    }

    public ProcessingException getException() {
        return exception;
    }

    public V getValue() {
        return value;
    }
}
