package org.flinkdsl.flow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.flinkdsl.flow.Flow;
import org.flinkdsl.flow.model.EnrichedEvent;
import org.flinkdsl.flow.model.FailedRecord;
import org.flinkdsl.flow.steps.Done;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.flinkdsl.kafka.KafkaJsonSink;
import org.flinkdsl.kafka.KafkaJsonSource;

import java.time.Duration;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
        String topic     = System.getenv().getOrDefault("KAFKA_TOPIC", "customers");
        String topicTo     = System.getenv().getOrDefault("KAFKA_TOPIC", "customers_to");
        String groupId     = System.getenv().getOrDefault("KAFKA_GROUP_ID", "customers_consumer");

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var source = KafkaJsonSource.builder(Customer.class, TypeInformation.of(Customer.class))
                .bootstrap(bootstrap)
                .topic(topic)
                .groupId(groupId)
                 .startingOffsets(OffsetsInitializer.latest())
                 //.eventTime(c -> c.getEventTimeMillis(), Duration.ofSeconds(15))
                .build();

        Flow.on(env)
                .from(source)
                .map((MapFunction<Customer, NotificationCustomer>) customer -> {
                    var notificationCustomer = new NotificationCustomer();
                    notificationCustomer.fullName = customer.firstName + " " + customer.lastName;

                    return  notificationCustomer;
                })
                .<EnrichedCustomer>asyncEnrich(new CustomEnricher(), Duration.ofSeconds(5), 10,false,TypeInformation.of(new TypeHint<EnrichedCustomer>() {}) )
                .to(new KafkaJsonSink<>(bootstrap, topicTo))
                .execute("flinkdsl-customer-consumer");

    }
}
