package com.stock.flink.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.stock.flink.model.RawKafkaRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StockDailyEventParserTest {

    private final StockDailyEventParser parser = new StockDailyEventParser(1);

    @Test
    void shouldParseValidEventAndRetainKafkaMetadata() {
        RawKafkaRecord raw = fixture("000001.SZ", validJson());

        var validated = parser.parse(raw);

        assertSame(raw, validated.raw());
        assertEquals("000001.SZ", validated.event().tsCode());
        assertEquals(LocalDate.of(2026, 7, 3), validated.event().tradeDate());
        assertEquals(OffsetDateTime.parse("2026-07-03T15:00:00+08:00"), validated.event().eventTime());
    }

    @Test
    void shouldRejectKafkaKeyDifferentFromTsCode() {
        RawKafkaRecord raw = fixture("OTHER", validJson());

        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(raw));

        assertEquals("KEY_MISMATCH", error.errorType());
    }

    @Test
    void shouldRejectMalformedJson() {
        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(fixture("000001.SZ", "{")));

        assertEquals("JSON_PARSE", error.errorType());
    }

    @Test
    void shouldRejectNullPayloadAsJsonParseError() {
        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(fixture("000001.SZ", null)));

        assertEquals("JSON_PARSE", error.errorType());
    }

    @Test
    void shouldRejectJsonNullAsJsonParseError() {
        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(fixture("000001.SZ", "null")));

        assertEquals("JSON_PARSE", error.errorType());
    }

    @Test
    void shouldRejectUnknownJsonField() {
        String json = validJson().replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"extra\":true");

        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(fixture("000001.SZ", json)));

        assertEquals("JSON_PARSE", error.errorType());
    }

    @Test
    void shouldRejectMissingRequiredField() {
        String json = validJson().replace("\"eventId\":\"event-1\"", "\"eventId\":null");

        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(fixture("000001.SZ", json)));

        assertEquals("VALIDATION", error.errorType());
        assertTrue(error.getMessage().contains("eventId"));
    }

    @Test
    void shouldRejectEveryRequiredFieldWhenNull() throws Exception {
        String[] required = {
                "eventId", "traceId", "tsCode", "source", "tradeDate", "open", "high", "low",
                "close", "preClose", "volume", "amount", "eventTime", "ingestTime", "schemaVersion"
        };
        ObjectMapper mapper = new ObjectMapper();
        for (String field : required) {
            ObjectNode json = (ObjectNode) mapper.readTree(validJson());
            json.putNull(field);
            String key = field.equals("tsCode") ? null : "000001.SZ";

            InvalidStockDailyEventException error = assertThrows(
                    InvalidStockDailyEventException.class,
                    () -> parser.parse(fixture(key, json.toString())), field);

            assertEquals("VALIDATION", error.errorType(), field);
            assertTrue(error.getMessage().contains(field), field);
        }
    }

    @Test
    void shouldRejectBlankIdentifiersAndSource() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        for (String field : new String[] {"eventId", "traceId", "tsCode", "source"}) {
            ObjectNode json = (ObjectNode) mapper.readTree(validJson());
            json.put(field, "  ");
            String key = field.equals("tsCode") ? "  " : "000001.SZ";

            InvalidStockDailyEventException error = assertThrows(
                    InvalidStockDailyEventException.class,
                    () -> parser.parse(fixture(key, json.toString())), field);

            assertEquals("VALIDATION", error.errorType(), field);
            assertTrue(error.getMessage().contains(field), field);
        }
    }

    @Test
    void shouldAllowNullableChangeAndPctChg() throws Exception {
        ObjectNode json = (ObjectNode) new ObjectMapper().readTree(validJson());
        json.putNull("change");
        json.putNull("pctChg");

        var validated = parser.parse(fixture("000001.SZ", json.toString()));

        assertNull(validated.event().change());
        assertNull(validated.event().pctChg());
    }

    @Test
    void shouldRejectUnsupportedSchemaVersion() {
        String json = validJson().replace("\"schemaVersion\":1", "\"schemaVersion\":2");

        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(fixture("000001.SZ", json)));

        assertEquals("VALIDATION", error.errorType());
        assertTrue(error.getMessage().contains("schemaVersion"));
    }

    @Test
    void shouldRejectNegativeVolumeAndAmount() {
        String json = validJson().replace("\"volume\":1250345", "\"volume\":-1")
                .replace("\"amount\":13054890.25", "\"amount\":-1");

        InvalidStockDailyEventException error = assertThrows(
                InvalidStockDailyEventException.class, () -> parser.parse(fixture("000001.SZ", json)));

        assertEquals("VALIDATION", error.errorType());
        assertTrue(error.getMessage().contains("volume"));
        assertTrue(error.getMessage().contains("amount"));
    }

    private RawKafkaRecord fixture(String key, String payload) {
        return new RawKafkaRecord("stock.ods.daily.v1", 2, 42L, 1_783_075_200_000L, key, payload);
    }

    private String validJson() {
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
