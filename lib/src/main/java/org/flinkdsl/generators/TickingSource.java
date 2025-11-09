package org.flinkdsl.generators;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction; // 2.1.x: legacy
import org.flinkdsl.flow.Flow;
import org.flinkdsl.util.SerializableSupplier;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TickingSource<T> implements Flow.Source<T> {

    private final long intervalMs;
    private final SerializableSupplier<T> supplier;
    private final TypeInformation<T> outType;

    public TickingSource(long intervalMs, SerializableSupplier<T> supplier, TypeInformation<T> outType) {
        this.intervalMs = intervalMs;
        this.supplier = Objects.requireNonNull(supplier);
        this.outType = Objects.requireNonNull(outType);
    }

    @Override
    public DataStreamSource<T> build(StreamExecutionEnvironment env) {
        DataStreamSource<T> ds = env.addSource(new Impl<>(intervalMs, supplier));
        ds.getTransformation().setOutputType(outType);
        return ds;
    }

    private static final class Impl<T> extends RichParallelSourceFunction<T> {
        private final long intervalMs;
        private final SerializableSupplier<T> supplier;
        private final AtomicBoolean running = new AtomicBoolean(true);

        Impl(long intervalMs, SerializableSupplier<T> supplier) {
            this.intervalMs = intervalMs;
            this.supplier = supplier;
        }

        @Override public void open(OpenContext openContext) throws Exception { }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();
            while (running.get()) {
                T v = supplier.get();
                synchronized (lock) { ctx.collect(v); }
                Thread.sleep(intervalMs);
            }
        }

        @Override public void cancel() { running.set(false); }
    }
}
