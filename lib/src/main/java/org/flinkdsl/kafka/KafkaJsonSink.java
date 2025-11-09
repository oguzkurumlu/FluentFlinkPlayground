package org.flinkdsl.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction; // 2.1.x legacy
import org.flinkdsl.flow.Flow;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

public final class KafkaJsonSink<T> implements Flow.Sink<T> {

    private final String bootstrap;
    private final String topic;

    public KafkaJsonSink(String bootstrap, String topic) {
        this.bootstrap = Objects.requireNonNull(bootstrap);
        this.topic = Objects.requireNonNull(topic);
    }

    @Override
    public void bind(DataStream<T> stream) {
        stream.addSink(new Impl<>(bootstrap, topic)).name("kafka-json-sink");
    }

    public static final class Impl<T> extends RichSinkFunction<T> {
        private final String bootstrap;
        private final String topic;
        private transient KafkaProducer<String, String> producer;
        private transient ObjectMapper mapper; // TM tarafında oluşturulacak

        public Impl(String bootstrap, String topic) {
            this.bootstrap = bootstrap; this.topic = topic;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            Properties p = new Properties();
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(ProducerConfig.ACKS_CONFIG, "1");
            p.put(ProducerConfig.LINGER_MS_CONFIG, "10");
            p.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
            p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<>(p);
            mapper = new ObjectMapper();
        }

        @Override
        public void invoke(T value, org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction.Context context) throws Exception {
            String key = Integer.toHexString(Objects.hashCode(value));
            String json = mapper.writeValueAsString(value);
            producer.send(new ProducerRecord<>(topic, key, json));
            producer.flush();
        }

        @Override public void close() { try { if (producer != null) producer.close(); } catch (Exception ignore) {} }
    }
}
