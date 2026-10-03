package com.stock.flink.calculation;

import com.stock.common.model.StockDailyEvent;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.flink.model.DailyPoint;
import com.stock.flink.model.ValidatedStockDailyEvent;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;

public final class IndicatorCalculator {
    private static final int SCALE = 6;

    public StockDailyIndicatorEvent calculate(
            List<DailyPoint> points, ValidatedStockDailyEvent input, OffsetDateTime calculationTime) {
        StockDailyEvent event = input.event();
        BigDecimal volMa5 = trailingAverage(points, 5, DailyPoint::volume);
        BigDecimal volumeRatio = volMa5 == null || volMa5.signum() == 0
                ? null : event.volume().divide(volMa5, SCALE, RoundingMode.HALF_UP);

        return new StockDailyIndicatorEvent(
                "FLINK_INDICATOR:%s:%s:v1".formatted(event.tsCode(), event.tradeDate()),
                event.eventId(), event.traceId(), event.tsCode(), event.tradeDate(),
                event.source(), event.eventTime(),
                scale(event.close()), scale(event.volume()), scale(event.pctChg()),
                trailingAverage(points, 5, DailyPoint::close),
                trailingAverage(points, 10, DailyPoint::close),
                trailingAverage(points, 20, DailyPoint::close),
                volMa5, trailingAverage(points, 10, DailyPoint::volume), volumeRatio,
                points.size(), points.size() < 20, calculationTime, 1);
    }

    static BigDecimal average(List<BigDecimal> values) {
        if (values.isEmpty()) {
            return null;
        }
        BigDecimal sum = values.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        return sum.divide(BigDecimal.valueOf(values.size()), SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal trailingAverage(
            List<DailyPoint> points, int period, Function<DailyPoint, BigDecimal> getter) {
        if (points.size() < period) {
            return null;
        }
        return average(points.subList(points.size() - period, points.size())
                .stream().map(getter).toList());
    }

    private static BigDecimal scale(BigDecimal value) {
        return value == null ? null : value.setScale(SCALE, RoundingMode.HALF_UP);
    }
}
