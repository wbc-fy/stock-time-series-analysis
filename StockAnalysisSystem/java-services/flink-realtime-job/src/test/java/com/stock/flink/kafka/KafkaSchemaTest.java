package com.stock.flink.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stock.common.model.FlinkDeadLetterEvent;
import com.stock.common.model.LateStockDailyEvent;
import com.stock.common.model.StockDailyEvent;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.flink.model.RawKafkaRecord;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaSchemaTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-09-30T10:15:30+08:00");

    @Test
    void shouldPreserveSourceMetadataKeyAndPayload() throws Exception {
        byte[] key = "000001.SZ".getBytes(StandardCharsets.UTF_8);
        byte[] payload = "{\"test\":true}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> source = new ConsumerRecord<>(
                "stock.ods.daily.v1", 2, 99L, 1_783_075_200_000L,
                TimestampType.CREATE_TIME, key.length, payload.length, key, payload,
                new RecordHeaders(), Optional.empty());
        List<RawKafkaRecord> output = new ArrayList<>();

        new RawKafkaRecordDeserializationSchema().deserialize(source, collector(output));

        assertEquals(1, output.size());
        assertEquals(new RawKafkaRecord("stock.ods.daily.v1", 2, 99L,
                1_783_075_200_000L, "000001.SZ", "{\"test\":true}"), output.getFirst());
    }

    @Test
    void shouldPreserveNullKafkaKeyAndPayload() throws Exception {
        ConsumerRecord<byte[], byte[]> source = new ConsumerRecord<>(
                "stock.ods.daily.v1", 0, 1L, null, null);
        List<RawKafkaRecord> output = new ArrayList<>();

        new RawKafkaRecordDeserializationSchema().deserialize(source, collector(output));

        assertNull(output.getFirst().key());
        assertNull(output.getFirst().payload());
    }

    @Test
    void shouldSerializeIndicatorWithTsCodeKeyAndJsonDates() throws Exception {
        StockDailyIndicatorEvent event = new StockDailyIndicatorEvent(
                "indicator-1", "event-1", "trace-1", "000001.SZ", LocalDate.parse("2026-07-03"),
                "TUSHARE", NOW, null, null, null, null, null, null, null, null, null,
                1, true, NOW, 1);

        ProducerRecord<byte[], byte[]> record =
                JsonKafkaRecordSerializationSchema.indicators("stock.dws.daily-indicator.v1")
                        .serialize(event, null, null);

        assertEquals("stock.dws.daily-indicator.v1", record.topic());
        assertArrayEquals("000001.SZ".getBytes(StandardCharsets.UTF_8), record.key());
        JsonNode json = JSON.readTree(record.value());
        assertEquals("000001.SZ", json.get("tsCode").asText());
        assertEquals("2026-07-03", json.get("tradeDate").asText());
    }

    @Test
    void shouldSerializeLateEventUsingOriginalTsCodeKey() throws Exception {
        StockDailyEvent original = new StockDailyEvent("event-1", "trace-1", "600000.SH",
                LocalDate.parse("2026-07-03"), null, null, null, null, null, null, null,
                null, null, "TUSHARE", NOW, NOW, 1);
        LateStockDailyEvent event = new LateStockDailyEvent(original,
                LocalDate.parse("2026-07-04"), "TRADE_DATE_BEFORE_LATEST", NOW, 1);

        ProducerRecord<byte[], byte[]> record =
                JsonKafkaRecordSerializationSchema.late("stock.late.daily.v1")
                        .serialize(event, null, null);

        assertEquals("stock.late.daily.v1", record.topic());
        assertArrayEquals("600000.SH".getBytes(StandardCharsets.UTF_8), record.key());
        assertEquals("600000.SH", JSON.readTree(record.value()).get("originalEvent").get("tsCode").asText());
    }

    @Test
    void shouldSerializeDeadLetterWithOriginalKey() throws Exception {
        FlinkDeadLetterEvent event = deadLetter("bad-key");

        ProducerRecord<byte[], byte[]> record =
                JsonKafkaRecordSerializationSchema.deadLetters("stock.flink.dead-letter.v1")
                        .serialize(event, null, null);

        assertEquals("stock.flink.dead-letter.v1", record.topic());
        assertArrayEquals("bad-key".getBytes(StandardCharsets.UTF_8), record.key());
        assertEquals("bad-key", JSON.readTree(record.value()).get("originalKey").asText());
    }

    @Test
    void shouldKeepNullDeadLetterKeyNull() throws Exception {
        ProducerRecord<byte[], byte[]> record =
                JsonKafkaRecordSerializationSchema.deadLetters("stock.flink.dead-letter.v1")
                        .serialize(deadLetter(null), null, null);

        assertNull(record.key());
        assertNull(JSON.readTree(record.value()).get("originalKey").textValue());
    }

    private static FlinkDeadLetterEvent deadLetter(String originalKey) {
        return new FlinkDeadLetterEvent("stock.ods.daily.v1", 2, 99L, originalKey,
                "{bad-json}", "JSON_PARSE", "invalid JSON", NOW, 1);
    }

    private static <T> Collector<T> collector(List<T> output) {
        return new Collector<>() {
            @Override
            public void collect(T record) {
                output.add(record);
            }

            @Override
            public void close() {
            }
        };
    }
}
