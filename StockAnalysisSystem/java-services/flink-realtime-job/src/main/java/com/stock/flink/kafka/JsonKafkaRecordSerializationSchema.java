package com.stock.flink.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stock.common.model.FlinkDeadLetterEvent;
import com.stock.common.model.LateStockDailyEvent;
import com.stock.common.model.StockDailyIndicatorEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class JsonKafkaRecordSerializationSchema<T>
        implements KafkaRecordSerializationSchema<T> {
    private static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String topic;
    private final KeyExtractor<T> keyExtractor;

    private JsonKafkaRecordSerializationSchema(String topic, KeyExtractor<T> keyExtractor) {
        this.topic = Objects.requireNonNull(topic);
        this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    public static JsonKafkaRecordSerializationSchema<StockDailyIndicatorEvent> indicators(String topic) {
        return new JsonKafkaRecordSerializationSchema<>(topic, StockDailyIndicatorEvent::tsCode);
    }

    public static JsonKafkaRecordSerializationSchema<LateStockDailyEvent> late(String topic) {
        return new JsonKafkaRecordSerializationSchema<>(topic, event -> event.originalEvent().tsCode());
    }

    public static JsonKafkaRecordSerializationSchema<FlinkDeadLetterEvent> deadLetters(String topic) {
        return new JsonKafkaRecordSerializationSchema<>(topic, FlinkDeadLetterEvent::originalKey);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, KafkaSinkContext context, Long timestamp) {
        String key = keyExtractor.key(element);
        try {
            return new ProducerRecord<>(topic,
                    key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                    JSON.writeValueAsBytes(element));
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not serialize Kafka output event", error);
        }
    }

    @FunctionalInterface
    private interface KeyExtractor<T> extends Serializable {
        String key(T element);
    }
}
