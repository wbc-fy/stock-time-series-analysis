package com.stock.common.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;

public record StockDailyIndicatorEvent(
        String eventId,
        String sourceEventId,
        String traceId,
        String tsCode,
        LocalDate tradeDate,
        String source,
        OffsetDateTime sourceEventTime,
        BigDecimal close,
        BigDecimal volume,
        BigDecimal pctChg,
        BigDecimal ma5,
        BigDecimal ma10,
        BigDecimal ma20,
        BigDecimal volMa5,
        BigDecimal volMa10,
        BigDecimal volumeRatio,
        Integer windowSize,
        Boolean isWarmup,
        OffsetDateTime calculationTime,
        Integer schemaVersion
) {
}
