package org.flinkdsl.produce;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public final class CustomerSerializer implements Serializer<Customer> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Customer data) {
        try {
            return data == null ? null : MAPPER.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Customer", e);
        }
    }

    @Override public void close() {}
}