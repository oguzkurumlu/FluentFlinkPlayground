package org.flinkdsl.flow.steps;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flinkdsl.flow.Flow;

public final class SourceStep<T> {
    private final StreamExecutionEnvironment env;
    private final DataStream<T> stream;

    public SourceStep(StreamExecutionEnvironment env, DataStream<T> stream) {
        this.env = env;
        this.stream = stream;
    }

    public SourceStep<T> withEventTime(WatermarkStrategy<T> wm) {
        DataStream<T> s = stream.assignTimestampsAndWatermarks(wm);
        return new SourceStep<>(env, s);
    }

    public <K> KeyedStep<T, K> keyBy(KeySelector<T, K> keySel, TypeInformation<K> keyType) {
        return new KeyedStep<>(env, stream.keyBy(keySel, keyType));
    }

    public <R> Step<R> map(MapFunction<T, R> fn) {
        return new Step<R>(env, stream.map(fn));
    }

    public Step<T> filter(FilterFunction<T> fn) {
        return new Step<>(env, stream.filter(fn));
    }

    public <R> Step<R> flatMap(FlatMapFunction<T, R> fn, TypeInformation<R> outType) {
        return new Step<>(env, stream.flatMap(fn).returns(outType));
    }

    public <S> Done to(Flow.Sink<S> sink) {
        @SuppressWarnings("unchecked")
        DataStream<S> s = (DataStream<S>) stream;
        sink.bind(s);
        return new Done(env);
    }

    public DataStream<T> asDataStream() { return stream; }
}
