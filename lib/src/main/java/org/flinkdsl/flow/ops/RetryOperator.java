package org.flinkdsl.flow.ops;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.time.Duration;
import java.util.function.Predicate;

public final class RetryOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T> {

    private final int maxAttempts;
    private final Duration backoff;
    private final Predicate<Throwable> when;

    public RetryOperator(int maxAttempts, Duration backoff, Predicate<Throwable> when) {
        this.maxAttempts = maxAttempts;
        this.backoff = backoff;
        this.when = when;
    }

    @Override
    public void processElement(StreamRecord<T> element) {

        output.collect(element);
    }
}
