package com.stock.flink.calculation;

import com.stock.common.model.StockDailyEvent;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.flink.model.DailyPoint;
import com.stock.flink.model.RawKafkaRecord;
import com.stock.flink.model.ValidatedStockDailyEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndicatorCalculatorTest {
    private static final LocalDate FIRST_DAY = LocalDate.of(2026, 8, 1);
    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-09-30T10:15:30+08:00");

    private final IndicatorCalculator calculator = new IndicatorCalculator();

    @ParameterizedTest
    @CsvSource({"1,true", "4,true", "5,true", "9,true", "10,true", "19,true", "20,false"})
    void exposesIndicatorsOnlyAfterTheirFullWindow(int size, boolean warmup) {
        StockDailyIndicatorEvent result = calculator.calculate(points(size), input(size), NOW);

        assertEquals(size, result.windowSize());
        assertEquals(warmup, result.isWarmup());
        assertEquals(size >= 5, result.ma5() != null);
        assertEquals(size >= 10, result.ma10() != null);
        assertEquals(size >= 20, result.ma20() != null);
        assertEquals(size >= 5, result.volMa5() != null);
        assertEquals(size >= 10, result.volMa10() != null);
        assertEquals(size >= 5, result.volumeRatio() != null);
    }

    @Test
    void averagesOnlyTheTrailingDatesInAscendingWindow() {
        StockDailyIndicatorEvent result = calculator.calculate(points(20), input(20), NOW);

        assertEquals(new BigDecimal("18.000000"), result.ma5());
        assertEquals(new BigDecimal("15.500000"), result.ma10());
        assertEquals(new BigDecimal("10.500000"), result.ma20());
        assertEquals(new BigDecimal("180.000000"), result.volMa5());
        assertEquals(new BigDecimal("155.000000"), result.volMa10());
        assertEquals(new BigDecimal("1.111111"), result.volumeRatio());
    }

    @Test
    void roundsHalfUpWithoutDoubleConversion() {
        assertEquals(new BigDecimal("1.666667"), IndicatorCalculator.average(List.of(
                new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("2"))));
        assertEquals(new BigDecimal("1.000001"), IndicatorCalculator.average(List.of(
                new BigDecimal("1.000000"), new BigDecimal("1.000001"))));
    }

    @Test
    void returnsNullForEmptyAverage() {
        assertNull(IndicatorCalculator.average(List.of()));
    }

    @Test
    void returnsNullRatioWhenVolumeAverageIsZero() {
        List<DailyPoint> points = new ArrayList<>();
        for (int day = 1; day <= 5; day++) {
            points.add(new DailyPoint(date(day), BigDecimal.valueOf(day), BigDecimal.ZERO));
        }

        StockDailyIndicatorEvent result = calculator.calculate(points, input(5), NOW);

        assertEquals(new BigDecimal("0.000000"), result.volMa5());
        assertNull(result.volumeRatio());
    }

    @Test
    void copiesContextAtSixDecimalPlacesAndUsesDeterministicId() {
        StockDailyIndicatorEvent result = calculator.calculate(points(1), input(1), NOW);

        assertEquals("FLINK_INDICATOR:000001.SZ:2026-08-01:v1", result.eventId());
        assertEquals("source-1", result.sourceEventId());
        assertEquals("trace-1", result.traceId());
        assertEquals("000001.SZ", result.tsCode());
        assertEquals(date(1), result.tradeDate());
        assertEquals("TUSHARE", result.source());
        assertEquals(OffsetDateTime.parse("2026-08-01T15:00:00+08:00"), result.sourceEventTime());
        assertEquals(new BigDecimal("1.000000"), result.close());
        assertEquals(new BigDecimal("10.000000"), result.volume());
        assertEquals(new BigDecimal("1.234568"), result.pctChg());
        assertEquals(NOW, result.calculationTime());
        assertEquals(1, result.schemaVersion());
        assertTrue(result.isWarmup());
    }

    @Test
    void preservesNullOptionalPercentChange() {
        StockDailyEvent event = event(1, null);
        StockDailyIndicatorEvent result = calculator.calculate(points(1), validated(event), NOW);

        assertNull(result.pctChg());
    }

    @Test
    void createsDailyPointFromValidatedEventValues() {
        DailyPoint point = DailyPoint.from(event(2, BigDecimal.ONE));

        assertEquals(date(2), point.tradeDate());
        assertEquals(new BigDecimal("2"), point.close());
        assertEquals(new BigDecimal("20"), point.volume());
    }

    private static List<DailyPoint> points(int size) {
        List<DailyPoint> points = new ArrayList<>();
        for (int day = 1; day <= size; day++) {
            points.add(new DailyPoint(date(day), BigDecimal.valueOf(day), BigDecimal.valueOf(day * 10L)));
        }
        return points;
    }

    private static ValidatedStockDailyEvent input(int day) {
        return validated(event(day, new BigDecimal("1.2345675")));
    }

    private static ValidatedStockDailyEvent validated(StockDailyEvent event) {
        return new ValidatedStockDailyEvent(
                new RawKafkaRecord("stock.ods.daily.v1", 0, 1L, 0L, event.tsCode(), "{}"), event);
    }

    private static StockDailyEvent event(int day, BigDecimal pctChg) {
        return new StockDailyEvent(
                "source-1", "trace-1", "000001.SZ", date(day),
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.valueOf(day),
                BigDecimal.ONE, BigDecimal.ZERO, pctChg, BigDecimal.valueOf(day * 10L),
                BigDecimal.ONE, "TUSHARE", date(day).atTime(15, 0).atOffset(java.time.ZoneOffset.ofHours(8)),
                NOW, 1);
    }

    private static LocalDate date(int day) {
        return FIRST_DAY.plusDays(day - 1L);
    }
}
