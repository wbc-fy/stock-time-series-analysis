package com.stock.flink.model;

import com.stock.common.model.StockDailyEvent;

import java.math.BigDecimal;
import java.time.LocalDate;

public record DailyPoint(LocalDate tradeDate, BigDecimal close, BigDecimal volume) {
    public static DailyPoint from(StockDailyEvent event) {
        return new DailyPoint(event.tradeDate(), event.close(), event.volume());
    }
}
