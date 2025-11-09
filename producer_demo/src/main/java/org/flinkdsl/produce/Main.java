package org.flinkdsl.produce;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flinkdsl.flow.Flow;
import org.flinkdsl.generators.TickingSource;
import org.flinkdsl.kafka.KafkaJsonSink;
import org.flinkdsl.util.SerializableSupplier;

public class Main {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
        String topic     = System.getenv().getOrDefault("KAFKA_TOPIC", "customers");
        long intervalMs  = Long.parseLong(System.getenv().getOrDefault("INTERVAL_MS", "5000"));

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SerializableSupplier<Customer> supplier = CustomerGenerator::next;

        Flow.on(env)
                .from(new TickingSource<>(
                        intervalMs,
                        supplier,
                        TypeInformation.of(new TypeHint<Customer>() {})))
                .to(new KafkaJsonSink<>(bootstrap, topic))
                .execute("flinkdsl-customer-producer");
    }
}