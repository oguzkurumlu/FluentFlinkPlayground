package org.flinkdsl.flow;

import org.flinkdsl.flow.steps.SourceStep;
import org.flinkdsl.flow.steps.Step;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class Flow {

    private final StreamExecutionEnvironment env;

    private Flow(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public static Flow on(StreamExecutionEnvironment env) {
        return new Flow(env);
    }

    public <T> SourceStep<T> from(Source<T> source) {
        DataStream<T> ds = source.build(env);
        return new SourceStep<>(env, ds);
    }

    @FunctionalInterface
    public interface Sink<T> {
        void bind(DataStream<T> stream);
    }

    @FunctionalInterface
    public interface Source<T> {
        DataStream<T> build(StreamExecutionEnvironment env);
    }
}
