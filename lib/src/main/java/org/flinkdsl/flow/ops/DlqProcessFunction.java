package org.flinkdsl.flow.ops;

import org.flinkdsl.flow.model.FailedRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public final class DlqProcessFunction<T> extends ProcessFunction<T, T> {
    private final OutputTag<FailedRecord<T>> dlqTag;

    public DlqProcessFunction(OutputTag<FailedRecord<T>> dlqTag) {
        this.dlqTag = Objects.requireNonNull(dlqTag, "dlqTag");
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
        try {
            out.collect(value);
        } catch (Exception ex) {
            ctx.output(dlqTag, new FailedRecord<>(value, "processing-failed", ex));
        }
    }
}
