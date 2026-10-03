package com.stock.market;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.stock.market.api.QueryParameters;
import com.stock.market.storage.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;

class RepositoryTest {
    @Test
    void preservesExactUnquotedClickHouseDecimalNumbers() {
        var client = mock(ClickHouseClient.class);
        when(client.execute(anyString(), anyMap(), eq("")))
                .thenReturn("{\"close\":12345678901234567890.123456,\"pct_chg\":-0.123456}");
        var row =
                new IndicatorRepository(client)
                        .query(QueryParameters.series("000001.SZ", 2, null, null))
                        .getFirst();
        assertThat((BigDecimal) row.get("close"))
                .isEqualByComparingTo("12345678901234567890.123456");
        assertThat((BigDecimal) row.get("pct_chg")).isEqualByComparingTo("-0.123456");
    }

    @Test
    void insertsEveryFieldAsNumericDecimalAndNullWithKafkaVersionAndUtcTime() {
        var client = mock(ClickHouseClient.class);
        when(client.execute(anyString(), anyMap(), anyString())).thenReturn("");
        new IndicatorRepository(client)
                .insert(
                        MarketBehaviorTest.event(),
                        new ConsumerRecord<>("topic", 2, 123, "000001.SZ", "json"));
        var body = ArgumentCaptor.forClass(String.class);
        verify(client)
                .execute(
                        eq("INSERT INTO daily_indicators FORMAT JSONEachRow"),
                        eq(Map.of()),
                        body.capture());
        assertThat(body.getValue())
                .contains(
                        "\"close\":10.000000",
                        "\"ma5\":null",
                        "\"source_event_time\":\"2026-09-01T07:00:00Z\"",
                        "\"kafka_partition\":2",
                        "\"kafka_offset\":123",
                        "\"version\":123",
                        "\"schema_version\":1");
    }

    @Test
    void mapsClickHouseDecimalStringsToNumbersAndPreservesNullsAndIsoDates() {
        var client = mock(ClickHouseClient.class);
        when(client.execute(anyString(), anyMap(), eq("")))
                .thenReturn(
                        "{\"trade_date\":\"2026-09-01\",\"close\":\"10.000000\",\"ma5\":null,\"is_warmup\":true,\"calculation_time\":\"2026-09-01"
                            + " 08:00:00.000000000\"}\n");
        var repository = new IndicatorRepository(client);
        var rows =
                repository.query(
                        QueryParameters.series(
                                "000001.SZ",
                                2,
                                LocalDate.of(2026, 8, 1),
                                LocalDate.of(2026, 9, 1)));
        assertThat(rows.getFirst())
                .containsEntry("close", new BigDecimal("10.000000"))
                .containsEntry("ma5", null)
                .containsEntry("is_warmup", true)
                .containsEntry("calculation_time", "2026-09-01T08:00:00.000000000Z");
        var sql = ArgumentCaptor.forClass(String.class);
        verify(client)
                .execute(
                        sql.capture(),
                        eq(
                                Map.of(
                                        "code",
                                        "000001.SZ",
                                        "limit",
                                        "2",
                                        "start",
                                        "2026-08-01",
                                        "end",
                                        "2026-09-01")),
                        eq(""));
        assertThat(sql.getValue())
                .contains(
                        "FINAL WHERE ts_code = {code:String}",
                        "ORDER BY trade_date DESC LIMIT {limit:UInt32}) ORDER BY trade_date ASC",
                        "{start:Date}",
                        "{end:Date}");
    }

    @Test
    void mysqlUsesBoundedPreparedQueriesAndConvertsDatesWithoutDroppingNull() {
        var jdbc = mock(JdbcTemplate.class);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("trade_date", java.sql.Date.valueOf("2026-09-01"));
        row.put("pct_chg", null);
        row.put("close", new BigDecimal("1.000000"));
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of(row));
        var repository = new MySqlMarketRepository(jdbc);
        assertThat(
                        repository
                                .bars(
                                        QueryParameters.series(
                                                "000001.SZ", 2, LocalDate.of(2026, 8, 1), null))
                                .getFirst())
                .containsEntry("trade_date", "2026-09-01")
                .containsEntry("pct_chg", null);
        verify(jdbc)
                .queryForList(
                        contains(
                                "ORDER BY trade_date DESC LIMIT ?) latest ORDER BY trade_date ASC"),
                        eq(new Object[] {"000001.SZ", LocalDate.of(2026, 8, 1), 2}));
    }

    @Test
    void malformedClickHouseResponseIsUnavailableNotPartialSuccess() {
        var client = mock(ClickHouseClient.class);
        when(client.execute(anyString(), anyMap(), anyString()))
                .thenReturn("{\"close\":1}\nnot json");
        assertThatThrownBy(
                        () ->
                                new IndicatorRepository(client)
                                        .query(QueryParameters.series("000001.SZ", 2, null, null)))
                .isInstanceOf(DependencyException.class);
    }
}
