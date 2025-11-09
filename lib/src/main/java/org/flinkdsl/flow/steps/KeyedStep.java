package org.flinkdsl.flow.steps;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.flinkdsl.flow.ops.DedupWithTtl;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public final class KeyedStep<T, K> extends Step<T> {
    private final KeyedStream<T, K> kstream;

    public KeyedStep(StreamExecutionEnvironment env, KeyedStream<T, K> ks) {
        super(env, ks);
        this.kstream = ks;
    }

    public Step<T> process(KeyedProcessFunction<K, T, T> fn) {
        return new Step<>(env, kstream.process(fn));
    }

    public Step<T> deduplicateByKey(java.time.Duration ttl) {
        DataStream<T> out = kstream.process(new org.flinkdsl.flow.ops.DedupWithTtl<>(ttl));
        out.getTransformation().setOutputType(kstream.getType());
        return new Step<>(env, out);
    }

    public <R> Step<R> map(MapFunction<T, R> fn, TypeInformation<R> outType) {
        return new Step<>(env, kstream.map(fn).returns(outType));
    }

    public KeyedStep<T, K> setParallelism(int p) {
        kstream.getTransformation().setParallelism(p);
        return this;
    }

    public KeyedStep<T, K> withUid(String uid) {
        kstream.getTransformation().setUid(uid);
        return this;
    }
}
