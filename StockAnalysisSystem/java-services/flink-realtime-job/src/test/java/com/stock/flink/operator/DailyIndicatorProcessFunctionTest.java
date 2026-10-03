package com.stock.flink.operator;

import com.stock.common.model.LateStockDailyEvent;
import com.stock.common.model.StockDailyEvent;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.flink.model.RawKafkaRecord;
import com.stock.flink.model.ValidatedStockDailyEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DailyIndicatorProcessFunctionTest {
    private static final LocalDate FIRST_DAY = LocalDate.of(2026, 8, 1);
    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-09-30T10:15:30+08:00");
    private KeyedOneInputStreamOperatorTestHarness<String, ValidatedStockDailyEvent, StockDailyIndicatorEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = newHarness();
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void shouldEmitAnIndicatorForEveryNewDate() throws Exception {
        harness.processElement(event("000001.SZ", 1, "10", "100"), 1L);
        harness.processElement(event("000001.SZ", 2, "11", "200"), 2L);

        assertEquals(2, mainOutput().size());
        assertEquals(2, mainOutput().getLast().windowSize());
        assertEquals(NOW, mainOutput().getLast().calculationTime());
        assertTrue(lateOutput().isEmpty());
    }

    @Test
    void shouldReplaceLatestDateWithoutGrowingWindow() throws Exception {
        harness.processElement(event("000001.SZ", 28, "10.00", "100"), 1L);
        harness.processElement(event("000001.SZ", 28, "11.00", "200"), 2L);

        assertEquals(2, mainOutput().size());
        assertEquals(1, mainOutput().getLast().windowSize());
        assertEquals(new BigDecimal("11.000000"), mainOutput().getLast().close());
    }

    @Test
    void shouldRouteOlderDateWithoutChangingState() throws Exception {
        harness.processElement(event("000001.SZ", 28, "10", "100"), 1L);
        harness.processElement(event("000001.SZ", 27, "9", "90"), 2L);
        harness.processElement(event("000001.SZ", 28, "11", "110"), 3L);

        assertEquals(2, mainOutput().size());
        assertEquals(1, mainOutput().getLast().windowSize());
        assertEquals(new BigDecimal("11.000000"), mainOutput().getLast().close());
        assertEquals(1, lateOutput().size());
        assertEquals(FIRST_DAY.plusDays(27), lateOutput().getFirst().latestTradeDate());
        assertEquals("TRADE_DATE_BEFORE_LATEST", lateOutput().getFirst().reason());
        assertEquals(FIRST_DAY.plusDays(26), lateOutput().getFirst().originalEvent().tradeDate());
        assertEquals(NOW, lateOutput().getFirst().processedAt());
        assertEquals(1, lateOutput().getFirst().schemaVersion());
    }

    @Test
    void shouldKeepKeysInSeparateWindows() throws Exception {
        harness.processElement(event("000001.SZ", 1, "10", "100"), 1L);
        harness.processElement(event("000002.SZ", 1, "20", "200"), 2L);
        harness.processElement(event("000001.SZ", 2, "11", "110"), 3L);

        assertEquals(List.of(1, 1, 2), mainOutput().stream().map(StockDailyIndicatorEvent::windowSize).toList());
        assertEquals("000001.SZ", mainOutput().getLast().tsCode());
    }

    @Test
    void shouldCapManagedWindowAtTwentyUniqueDates() throws Exception {
        for (int day = 1; day <= 21; day++) {
            harness.processElement(event("000001.SZ", day, Integer.toString(day), "100"), day);
        }

        StockDailyIndicatorEvent last = mainOutput().getLast();
        assertEquals(21, mainOutput().size());
        assertEquals(20, last.windowSize());
        assertEquals(new BigDecimal("11.500000"), last.ma20());
        assertEquals(false, last.isWarmup());
    }

    @Test
    void shouldRestoreManagedWindowAndLatestDateFromSnapshot() throws Exception {
        for (int day = 1; day <= 10; day++) {
            harness.processElement(event("000001.SZ", day, Integer.toString(day), "100"), day);
        }
        OperatorSubtaskState snapshot = harness.snapshot(1L, 10L);
        harness.processElement(event("000001.SZ", 11, "11", "100"), 11L);
        StockDailyIndicatorEvent uninterrupted = mainOutput().getLast();

        try (var restored = newHarness()) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(event("000001.SZ", 9, "9", "100"), 12L);
            restored.processElement(event("000001.SZ", 11, "11", "100"), 13L);

            assertEquals(1, restored.getSideOutput(FlinkOutputTags.LATE).size());
            StockDailyIndicatorEvent afterRestore = restored.extractOutputValues().getLast();
            assertEquals(11, afterRestore.windowSize());
            assertEquals(uninterrupted.ma10(), afterRestore.ma10());
            assertEquals(uninterrupted.close(), afterRestore.close());
        }
    }

    private List<StockDailyIndicatorEvent> mainOutput() {
        return harness.extractOutputValues();
    }

    private List<LateStockDailyEvent> lateOutput() {
        var output = harness.getSideOutput(FlinkOutputTags.LATE);
        return output == null ? List.of() : output.stream().map(record -> record.getValue()).toList();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, ValidatedStockDailyEvent, StockDailyIndicatorEvent>
            newHarness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DailyIndicatorProcessFunction(() -> NOW)),
                input -> input.event().tsCode(), Types.STRING);
    }

    private static ValidatedStockDailyEvent event(String tsCode, int day, String close, String volume) {
        LocalDate date = FIRST_DAY.plusDays(day - 1L);
        StockDailyEvent stock = new StockDailyEvent(
                "event-" + day, "trace-" + day, tsCode, date,
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, new BigDecimal(close),
                BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ZERO, new BigDecimal(volume),
                BigDecimal.ONE, "TUSHARE", date.atTime(15, 0).atOffset(ZoneOffset.ofHours(8)), NOW, 1);
        return new ValidatedStockDailyEvent(
                new RawKafkaRecord("stock.ods.daily.v1", 0, day, day, tsCode, "{}"), stock);
    }
}
