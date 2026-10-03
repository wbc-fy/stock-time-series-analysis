package com.stock.common.model;

import java.time.LocalDate;
import java.time.OffsetDateTime;

public record LateStockDailyEvent(
        StockDailyEvent originalEvent,
        LocalDate latestTradeDate,
        String reason,
        OffsetDateTime processedAt,
        Integer schemaVersion
) {
}
