package com.stock.common.model;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlinkSideOutputEventJsonTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void shouldRoundTripLateEventWithOriginalDailyEvent() throws Exception {
        StockDailyEvent original = objectMapper.readValue(
                Files.readString(Path.of("..", "..", "docs", "examples", "stock_daily_event_v1.json")),
                StockDailyEvent.class);
        LateStockDailyEvent event = new LateStockDailyEvent(
                original, LocalDate.of(2026, 8, 28), "OUTSIDE_RECOMPUTE_HORIZON",
                OffsetDateTime.parse("2026-08-28T16:30:00+08:00"), 1);

        LateStockDailyEvent decoded = objectMapper.readValue(
                objectMapper.writeValueAsString(event), LateStockDailyEvent.class);

        assertEquals(original, decoded.originalEvent());
        assertEquals(LocalDate.of(2026, 8, 28), decoded.latestTradeDate());
        assertEquals("OUTSIDE_RECOMPUTE_HORIZON", decoded.reason());
        assertEquals(OffsetDateTime.parse("2026-08-28T16:30:00+08:00"), decoded.processedAt());
        assertEquals(1, decoded.schemaVersion());
    }

    @Test
    void shouldPreserveOriginalDeadLetterPayload() throws Exception {
        FlinkDeadLetterEvent event = new FlinkDeadLetterEvent(
                "stock.ods.daily.v1", 1, 42L, "bad-key", "{bad-json}",
                "JSON_PARSE", "invalid JSON", OffsetDateTime.parse("2026-08-28T16:30:00+08:00"), 1);

        FlinkDeadLetterEvent decoded = objectMapper.readValue(
                objectMapper.writeValueAsString(event), FlinkDeadLetterEvent.class);

        assertEquals("stock.ods.daily.v1", decoded.originalTopic());
        assertEquals(1, decoded.originalPartition());
        assertEquals(42L, decoded.originalOffset());
        assertEquals("bad-key", decoded.originalKey());
        assertEquals("{bad-json}", decoded.originalPayload());
        assertEquals("JSON_PARSE", decoded.errorType());
        assertEquals("invalid JSON", decoded.errorMessage());
        assertEquals(OffsetDateTime.parse("2026-08-28T16:30:00+08:00"), decoded.failedAt());
        assertEquals(1, decoded.schemaVersion());
    }
}
