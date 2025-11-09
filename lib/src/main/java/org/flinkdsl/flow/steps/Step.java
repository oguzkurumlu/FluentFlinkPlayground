package org.flinkdsl.flow.steps;

import org.flinkdsl.flow.AsyncEnricher;
import org.flinkdsl.flow.Flow.Sink;
import org.flinkdsl.flow.model.FailedRecord;
import org.flinkdsl.flow.ops.DlqProcessFunction;
import org.flinkdsl.flow.ops.RetryOperator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class Step<T> {
    protected final StreamExecutionEnvironment env;
    protected final DataStream<T> stream;

    public Step(StreamExecutionEnvironment env, DataStream<T> stream) {
        this.env = env;
        this.stream = stream;
    }

    public <R> Step<R> map(org.apache.flink.api.common.functions.MapFunction<T, R> fn,
                           org.apache.flink.api.common.typeinfo.TypeInformation<R> outType) {
        DataStream<R> out = stream.map(fn);
        out.getTransformation().setOutputType(outType);
        return new Step<>(env, out);
    }

    public Step<T> filter(org.apache.flink.api.common.functions.FilterFunction<T> fn) {
        DataStream<T> out = stream.filter(fn);
        out.getTransformation().setOutputType(stream.getType());
        return new Step<>(env, out);
    }

    public <R> Step<R> flatMap(org.apache.flink.api.common.functions.FlatMapFunction<T, R> fn,
                               org.apache.flink.api.common.typeinfo.TypeInformation<R> outType) {
        DataStream<R> out = stream.flatMap(fn);
        out.getTransformation().setOutputType(outType);
        return new Step<>(env, out);
    }

    public <R> Step<R> asyncEnrich(AsyncEnricher<T, R> enricher,
                                   java.time.Duration timeout,
                                   int capacity,
                                   boolean ordered,
                                   TypeInformation<R> outType) {
        DataStream<R> out = ordered
                ? AsyncDataStream.orderedWait(stream, enricher, timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS, capacity)
                : AsyncDataStream.unorderedWait(stream, enricher, timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS, capacity);

        out.getTransformation().setOutputType(outType);
        return new Step<>(env, out);
    }

    public Step<T> deadLetter(org.apache.flink.util.OutputTag<org.flinkdsl.flow.model.FailedRecord<T>> dlqTag) {
        DataStream<T> out = stream.process(new org.flinkdsl.flow.ops.DlqProcessFunction<>(dlqTag));
        out.getTransformation().setOutputType(stream.getType());
        return new Step<>(env, out);
    }

    public Step<T> retry(int maxAttempts, Duration backoff, Predicate<Throwable> when) {
        DataStream<T> out = stream.transform("retry", stream.getType(),
                new RetryOperator<>(maxAttempts, backoff, when));
        return new Step<>(env, out);
    }

    public Step<T> withUid(String uid) {
        stream.getTransformation().setUid(uid);
        return this;
    }

    public Step<T> setParallelism(int p) {
        stream.getTransformation().setParallelism(p);
        return this;
    }

    public Done to(org.flinkdsl.flow.Flow.Sink<T> sink) {
        sink.bind(stream);
        return new Done(env);
    }

    public DataStream<T> asDataStream() { return stream; }
}
