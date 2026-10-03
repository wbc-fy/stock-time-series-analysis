package com.stock.common.model;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StockDailyIndicatorEventJsonTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void shouldDeserializeCanonicalIndicatorFixture() throws Exception {
        StockDailyIndicatorEvent event = objectMapper.readValue(fixtureJson(), StockDailyIndicatorEvent.class);

        assertEquals("FLINK_INDICATOR:000001.SZ:2026-08-28:v1", event.eventId());
        assertEquals("TUSHARE:000001.SZ:20260828", event.sourceEventId());
        assertEquals("replay-20260801-20260828", event.traceId());
        assertEquals("000001.SZ", event.tsCode());
        assertEquals(LocalDate.of(2026, 8, 28), event.tradeDate());
        assertEquals("TUSHARE", event.source());
        assertEquals(OffsetDateTime.parse("2026-08-28T15:00:00+08:00"), event.sourceEventTime());
        assertEquals(new BigDecimal("10.550000"), event.close());
        assertEquals(new BigDecimal("1250345.000000"), event.volume());
        assertEquals(new BigDecimal("3.431400"), event.pctChg());
        assertEquals(new BigDecimal("10.330000"), event.ma5());
        assertEquals(new BigDecimal("10.210000"), event.ma10());
        assertEquals(new BigDecimal("9.980000"), event.ma20());
        assertEquals(new BigDecimal("1200000.000000"), event.volMa5());
        assertEquals(new BigDecimal("1150000.000000"), event.volMa10());
        assertEquals(new BigDecimal("1.041954"), event.volumeRatio());
        assertEquals(20, event.windowSize());
        assertFalse(event.isWarmup());
        assertEquals(OffsetDateTime.parse("2026-08-28T16:30:00+08:00"), event.calculationTime());
        assertEquals(1, event.schemaVersion());
    }

    @Test
    void shouldRoundTripIndicatorFixtureWithoutChangingJsonFieldNames() throws Exception {
        StockDailyIndicatorEvent event = objectMapper.readValue(fixtureJson(), StockDailyIndicatorEvent.class);

        assertEquals(objectMapper.readTree(fixtureJson()), objectMapper.readTree(objectMapper.writeValueAsString(event)));
    }

    @Test
    void shouldRejectUnknownIndicatorField() throws Exception {
        String json = fixtureJson().replace("\"schemaVersion\": 1", "\"schemaVersion\": 1, \"unexpected\": true");

        assertThrows(UnrecognizedPropertyException.class,
                () -> objectMapper.readValue(json, StockDailyIndicatorEvent.class));
    }

    private String fixtureJson() throws Exception {
        return Files.readString(Path.of("..", "..", "docs", "examples", "stock_daily_indicator_event_v1.json"));
    }
}
