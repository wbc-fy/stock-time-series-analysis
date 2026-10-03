package com.stock.flink.model;

public record RawKafkaRecord(
        String topic,
        int partition,
        long offset,
        long timestamp,
        String key,
        String payload
) {
}
