package org.example;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lite.Lite;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
        String topic     = System.getenv().getOrDefault("KAFKA_TOPIC", "customers");
        String topicTo     = System.getenv().getOrDefault("KAFKA_TOPIC", "customers_to");
        String groupId     = System.getenv().getOrDefault("KAFKA_GROUP_ID", "customers_consumer");

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        var source = Lite.fromKafkaJson(env, bootstrap, topic, groupId, Customer.class).
                        map(c -> new NotificationCustomer());

        var enrichedTypeInfo = TypeInformation.of(new TypeHint<EnrichedCustomer>() {});
        var finalStream = Lite.asyncUnordered(source, new CustomEnricher(), Duration.ofSeconds(10), 10, enrichedTypeInfo);

        Lite.toKafkaJson(finalStream, bootstrap, topicTo);

        env.execute("Consumer with Lite library");
    }
}