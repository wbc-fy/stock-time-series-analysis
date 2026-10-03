package com.stock.flink.operator;

import com.stock.common.model.FlinkDeadLetterEvent;
import com.stock.flink.model.RawKafkaRecord;
import com.stock.flink.model.ValidatedStockDailyEvent;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParseAndValidateProcessFunctionTest {
    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-09-30T10:15:30+08:00");
    private OneInputStreamOperatorTestHarness<RawKafkaRecord, ValidatedStockDailyEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(new ParseAndValidateProcessFunction(1, () -> NOW)));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void shouldEmitValidatedEventWithOriginalKafkaRecord() throws Exception {
        RawKafkaRecord raw = raw("000001.SZ", validJson());

        harness.processElement(raw, 1L);

        assertEquals(raw, harness.extractOutputValues().getFirst().raw());
        assertEquals("000001.SZ", harness.extractOutputValues().getFirst().event().tsCode());
        assertTrue(deadLetters().isEmpty());
    }

    @Test
    void shouldRouteInvalidJsonToDeadLetterWithOriginalEnvelope() throws Exception {
        RawKafkaRecord raw = raw("000001.SZ", "{bad-json}");

        harness.processElement(raw, 1L);

        assertTrue(harness.extractOutputValues().isEmpty());
        FlinkDeadLetterEvent deadLetter = deadLetters().getFirst();
        assertEquals("JSON_PARSE", deadLetter.errorType());
        assertEquals(raw.topic(), deadLetter.originalTopic());
        assertEquals(raw.partition(), deadLetter.originalPartition());
        assertEquals(raw.offset(), deadLetter.originalOffset());
        assertEquals(raw.key(), deadLetter.originalKey());
        assertEquals(raw.payload(), deadLetter.originalPayload());
        assertEquals(NOW, deadLetter.failedAt());
        assertEquals(1, deadLetter.schemaVersion());
    }

    @Test
    void shouldRouteKeyMismatchWithoutLosingOriginalKafkaKeyOrPayload() throws Exception {
        RawKafkaRecord raw = raw("OTHER", validJson());

        harness.processElement(raw, 1L);

        assertTrue(harness.extractOutputValues().isEmpty());
        FlinkDeadLetterEvent deadLetter = deadLetters().getFirst();
        assertEquals("KEY_MISMATCH", deadLetter.errorType());
        assertEquals("OTHER", deadLetter.originalKey());
        assertEquals(raw.payload(), deadLetter.originalPayload());
    }

    @Test
    void shouldRouteValidationFailureToDeadLetter() throws Exception {
        RawKafkaRecord raw = raw("000001.SZ", validJson().replace("\"eventId\":\"event-1\"", "\"eventId\":null"));

        harness.processElement(raw, 1L);

        assertTrue(harness.extractOutputValues().isEmpty());
        assertEquals("VALIDATION", deadLetters().getFirst().errorType());
        assertEquals(raw.payload(), deadLetters().getFirst().originalPayload());
    }

    @Test
    void shouldRouteNegativeCloseToDeadLetterBeforeKeyedState() throws Exception {
        RawKafkaRecord raw = raw("000001.SZ", validJson().replace("\"close\":10.55", "\"close\":-0.01"));

        harness.processElement(raw, 1L);

        assertTrue(harness.extractOutputValues().isEmpty(),
                "a rejected event must not reach the valid stream that feeds keyed state");
        assertEquals(1, deadLetters().size());
        assertEquals("VALIDATION", deadLetters().getFirst().errorType());
        assertTrue(deadLetters().getFirst().errorMessage().contains("close"));
    }

    private List<FlinkDeadLetterEvent> deadLetters() {
        var output = harness.getSideOutput(FlinkOutputTags.DEAD_LETTER);
        return output == null ? List.of() : output.stream().map(record -> record.getValue()).toList();
    }

    private static RawKafkaRecord raw(String key, String payload) {
        return new RawKafkaRecord("stock.ods.daily.v1", 2, 42L, 1_783_075_200_000L, key, payload);
    }

    private static String validJson() {
        return """
                {"eventId":"event-1","traceId":"trace-1","tsCode":"000001.SZ",
                "tradeDate":"2026-07-03","open":10.25,"high":10.68,"low":10.12,
                "close":10.55,"preClose":10.20,"change":0.35,"pctChg":3.4314,
                "volume":1250345,"amount":13054890.25,"source":"TUSHARE",
                "eventTime":"2026-07-03T15:00:00+08:00",
                "ingestTime":"2026-07-03T16:20:30+08:00","schemaVersion":1}
                """;
    }
}
