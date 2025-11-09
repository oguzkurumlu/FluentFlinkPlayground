package org.lite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class Lite {
    private static final ObjectMapper M = new ObjectMapper();
    private Lite() {}

    public static <T> DataStream<T> fromKafkaJson(
            StreamExecutionEnvironment env,
            String bootstrap, String topic, String groupId, Class<T> clazz) {

        KafkaSource<String> src = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(src, WatermarkStrategy.noWatermarks(), "kafka-json")
                .map(s -> M.readValue(s, clazz))
                .returns(TypeInformation.of(clazz))
                .name("json->" + clazz.getSimpleName());
    }

    public static <T> DataStream<T> fromKafkaJson(
            StreamExecutionEnvironment env,
            String bootstrap, String topic, String groupId, Class<T> clazz,
            Function<T, Long> tsExtractor, Duration outOfOrderness) {

        KafkaSource<String> src = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        var parsed = env.fromSource(src, WatermarkStrategy.noWatermarks(), "kafka-json")
                .map(s -> M.readValue(s, clazz))
                .returns(TypeInformation.of(clazz));

        return parsed.assignTimestampsAndWatermarks(
                org.apache.flink.api.common.eventtime.WatermarkStrategy
                        .<T>forBoundedOutOfOrderness(outOfOrderness)
                        .withTimestampAssigner((e, ts) -> tsExtractor.apply(e))
        ).name("assign-ts+wm");
    }

    public static <I,O> DataStream<O> map(DataStream<I> in, Function<I,O> fn) {
        return in.map(fn::apply).returns(TypeInformation.of(new TypeHint<O>(){}));
    }

    private static <I,O> DataStream<O> asyncInternal(
            DataStream<I> in,
            AsyncEnricher<I,O> fn,
            Duration timeout, int capacity, boolean ordered, TypeInformation<O> outType) {

        var f = new AsyncFunction<I,O>() {
            @Override public void asyncInvoke(I input, ResultFuture<O> rf) {
                fn.apply(input).whenComplete((val, ex) -> {
                    if (ex != null) rf.complete(Collections.emptyList());
                    else rf.complete(Collections.singletonList(val));
                });
            }
        };

        var stream = ordered
                ? AsyncDataStream.orderedWait(in, f, timeout.toMillis(), TimeUnit.MILLISECONDS, capacity)
                : AsyncDataStream.unorderedWait(in, f, timeout.toMillis(), TimeUnit.MILLISECONDS, capacity);

        return stream.returns(outType);
    }

    public static <I,O> DataStream<O> asyncOrdered(
            DataStream<I> in,
            AsyncEnricher<I,O> fn,
            Duration timeout, int capacity, TypeInformation<O> outType) {

        return asyncInternal(in, fn, timeout, capacity, true, outType);
    }

    public static <I,O> DataStream<O> asyncUnordered(
            DataStream<I> in,
            AsyncEnricher<I,O> fn,
            Duration timeout, int capacity, TypeInformation<O> outType) {

        return asyncInternal(in, fn, timeout, capacity, false, outType);
    }

    public static <T> void toKafkaJson(DataStream<T> stream, String bootstrap, String topic) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                //.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // ihtiyaca göre
                .setKafkaProducerConfig(acksAll())
                .build();

        stream
                .map(v -> M.writeValueAsString(v))
                .returns(TypeInformation.of(String.class))
                .name("to-json")
                .sinkTo(sink)
                .name("kafka-sink");
    }

    private static Properties acksAll() {
        Properties p = new Properties();
        p.setProperty("acks","all");
        return p;
    }
}
