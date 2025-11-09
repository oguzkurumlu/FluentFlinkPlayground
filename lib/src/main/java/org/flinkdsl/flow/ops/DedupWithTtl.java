package org.flinkdsl.flow.ops;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public final class DedupWithTtl<K, T> extends KeyedProcessFunction<K, T, T> {

    private final long ttlMillis;
    private transient ValueState<Long> lastSeenTs;

    public DedupWithTtl(Duration ttl) {
        this.ttlMillis = ttl.toMillis();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Long> desc =
                new ValueStateDescriptor<>("dedup-last-seen", Long.class);

        StateTtlConfig ttlCfg = StateTtlConfig
                .newBuilder(Duration.ofMillis(ttlMillis))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        desc.enableTimeToLive(ttlCfg);
        lastSeenTs = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
        long ts = ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis();
        Long prev = lastSeenTs.value();

        if (prev == null || (ts - prev) > ttlMillis) {
            lastSeenTs.update(ts);
            out.collect(value);
        }
    }
}
