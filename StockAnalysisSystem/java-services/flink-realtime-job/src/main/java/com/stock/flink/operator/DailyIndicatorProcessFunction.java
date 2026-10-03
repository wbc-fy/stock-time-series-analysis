package com.stock.flink.operator;

import com.stock.common.model.LateStockDailyEvent;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.flink.calculation.IndicatorCalculator;
import com.stock.flink.model.DailyPoint;
import com.stock.flink.model.ValidatedStockDailyEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public final class DailyIndicatorProcessFunction
        extends KeyedProcessFunction<String, ValidatedStockDailyEvent, StockDailyIndicatorEvent> {
    private static final int MAX_WINDOW_SIZE = 20;

    private final ProcessingClock clock;
    private transient IndicatorCalculator calculator;
    private transient ListState<DailyPoint> pointsState;
    private transient ValueState<LocalDate> latestDateState;

    public DailyIndicatorProcessFunction(ProcessingClock clock) {
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public void open(OpenContext context) {
        calculator = new IndicatorCalculator();
        pointsState = getRuntimeContext().getListState(new ListStateDescriptor<>(
                "daily-points", TypeInformation.of(DailyPoint.class)));
        latestDateState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "latest-trade-date", LocalDate.class));
    }

    @Override
    public void processElement(
            ValidatedStockDailyEvent input, Context ctx, Collector<StockDailyIndicatorEvent> out)
            throws Exception {
        LocalDate tradeDate = input.event().tradeDate();
        LocalDate latest = latestDateState.value();
        if (latest != null && tradeDate.isBefore(latest)) {
            ctx.output(FlinkOutputTags.LATE, new LateStockDailyEvent(
                    input.event(), latest, "TRADE_DATE_BEFORE_LATEST", clock.now(), 1));
            return;
        }

        List<DailyPoint> points = new ArrayList<>();
        for (DailyPoint point : pointsState.get()) {
            points.add(point);
        }
        if (latest != null && tradeDate.equals(latest)) {
            points.set(points.size() - 1, DailyPoint.from(input.event()));
        } else {
            points.add(DailyPoint.from(input.event()));
            points.sort(Comparator.comparing(DailyPoint::tradeDate));
            if (points.size() > MAX_WINDOW_SIZE) {
                points.remove(0);
            }
        }
        pointsState.update(points);
        latestDateState.update(tradeDate);
        out.collect(calculator.calculate(points, input, clock.now()));
    }
}
