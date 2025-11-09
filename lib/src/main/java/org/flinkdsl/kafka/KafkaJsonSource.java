package org.flinkdsl.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource; // Flink 1.15+ 'fromSource' çoğu sürümde DataStreamSource döndürür
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flinkdsl.flow.Flow;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.function.ToLongFunction;

public final class KafkaJsonSource<T> implements Flow.Source<T> {

    private final String bootstrap;
    private final String topic;
    private final String groupId;
    private final Class<T> clazz;
    private final TypeInformation<T> outType;

    // optional
    private final OffsetsInitializer startingOffsets;
    private final Properties extraKafkaProps;

    // event-time config (optional)
    private final ToLongFunction<T> timestampExtractor; // event timestamp in millis
    private final Duration outOfOrderness;              // watermark lag
    private final boolean useEventTime;

    private KafkaJsonSource(Builder<T> b) {
        this.bootstrap = Objects.requireNonNull(b.bootstrap, "bootstrap");
        this.topic = Objects.requireNonNull(b.topic, "topic");
        this.groupId = Objects.requireNonNull(b.groupId, "groupId");
        this.clazz = Objects.requireNonNull(b.clazz, "clazz");
        this.outType = Objects.requireNonNull(b.outType, "outType");
        this.startingOffsets = b.startingOffsets != null ? b.startingOffsets : OffsetsInitializer.earliest();
        this.extraKafkaProps = b.extraKafkaProps;
        this.timestampExtractor = b.timestampExtractor;
        this.outOfOrderness = b.outOfOrderness != null ? b.outOfOrderness : Duration.ofSeconds(10);
        this.useEventTime = b.timestampExtractor != null;
    }

    public static <T> Builder<T> builder(Class<T> clazz, TypeInformation<T> outType) {
        return new Builder<>(clazz, outType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataStreamSource<T> build(StreamExecutionEnvironment env) {
        // 1) KafkaSource (value: String)
        KafkaSource<String> src = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(startingOffsets)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // Ek Kafka ayarları (security, max.poll vb.)
                .setProperties(extraKafkaProps == null ? new Properties() : extraKafkaProps)
                .build();

        // 2) Kaynağı ekle: burada watermark'ı String seviyesinde vermiyoruz.
        //    Event-time kullanacaksak parse sonrası T üzerinde timestamp/watermark uygulayacağız.
        DataStream<String> raw =
                env.fromSource(src, WatermarkStrategy.noWatermarks(), "kafka-json");

        // 3) JSON -> T
        ObjectMapper mapper = new ObjectMapper();
        DataStream<T> parsed = raw
                .map(json -> mapper.readValue(json, clazz))
                .returns(outType)
                .name("json->" + clazz.getSimpleName());

        // 4) (Opsiyonel) event-time timestamp & watermark
        if (useEventTime) {
            WatermarkStrategy<T> wm = WatermarkStrategy
                    .<T>forBoundedOutOfOrderness(outOfOrderness)
                    .withTimestampAssigner((SerializableTimestampAssigner<T>) (e, ts) -> timestampExtractor.applyAsLong(e));

            parsed = parsed
                    .assignTimestampsAndWatermarks(wm)
                    .name("assign-ts+wm");
        }

        // Çoğu Flink 1.15+ sürümünde fromSource -> DataStreamSource döner;
        // bazı 2.x sürümlerinde DataStream dönebilir. Flow.Source<T> için DataStreamSource gerekli ise cast uygundur.
        return (parsed instanceof DataStreamSource)
                ? (DataStreamSource<T>) parsed
                : (DataStreamSource<T>) (Object) parsed; // güvenli: runtime tip genelde DataStreamSource
    }

    // ---------- Builder ----------
    public static final class Builder<T> {
        private final Class<T> clazz;
        private final TypeInformation<T> outType;
        private String bootstrap;
        private String topic;
        private String groupId;
        private OffsetsInitializer startingOffsets;
        private Properties extraKafkaProps;
        private ToLongFunction<T> timestampExtractor;
        private Duration outOfOrderness;

        private Builder(Class<T> clazz, TypeInformation<T> outType) {
            this.clazz = clazz;
            this.outType = outType;
        }

        public Builder<T> bootstrap(String bootstrap) {
            this.bootstrap = bootstrap; return this;
        }
        public Builder<T> topic(String topic) {
            this.topic = topic; return this;
        }
        public Builder<T> groupId(String groupId) {
            this.groupId = groupId; return this;
        }
        public Builder<T> startingOffsets(OffsetsInitializer starting) {
            this.startingOffsets = starting; return this;
        }
        /** Ek Kafka ayarları: security.protocol, sasl.mechanism, max.poll.records, fetch.max.bytes vs. */
        public Builder<T> extraKafkaProps(Properties props) {
            this.extraKafkaProps = props; return this;
        }

        /** Event-time kullanmak istiyorsan T içinden epochMillis döndüren extractor ver. */
        public Builder<T> eventTime(ToLongFunction<T> tsExtractor, Duration outOfOrderness) {
            this.timestampExtractor = tsExtractor;
            this.outOfOrderness = outOfOrderness; return this;
        }

        public KafkaJsonSource<T> build() {
            return new KafkaJsonSource<>(this);
        }
    }
}
