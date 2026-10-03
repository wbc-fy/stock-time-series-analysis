package com.stock.flink.model;

import com.stock.common.model.StockDailyEvent;

public record ValidatedStockDailyEvent(RawKafkaRecord raw, StockDailyEvent event) {
}
