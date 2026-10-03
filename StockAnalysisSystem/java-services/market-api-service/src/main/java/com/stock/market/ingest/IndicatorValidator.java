package com.stock.market.ingest;

import com.stock.common.model.StockDailyIndicatorEvent;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Objects;

@Component
public class IndicatorValidator {
    private final String topic;

    public IndicatorValidator(@Value("${market.indicator-topic}") String topic) {
        this.topic = topic;
    }

    public void validate(StockDailyIndicatorEvent e, String key, String receivedTopic) {
        if (e == null) bad();
        text(e.eventId());
        text(e.sourceEventId());
        text(e.traceId());
        text(e.tsCode());
        text(e.source());
        if (!Objects.equals(key, e.tsCode())
                || !topic.equals(receivedTopic)
                || !Integer.valueOf(1).equals(e.schemaVersion())) bad();
        if (e.tradeDate() == null
                || e.sourceEventTime() == null
                || e.calculationTime() == null
                || e.isWarmup() == null
                || e.windowSize() == null
                || e.windowSize() < 1
                || e.windowSize() > 20
                || e.isWarmup() != (e.windowSize() < 20)) bad();
        decimal(e.close(), true, true);
        decimal(e.volume(), true, true);
        decimal(e.pctChg(), false, false);
        warm(e.ma5(), 5, e.windowSize(), true);
        warm(e.ma10(), 10, e.windowSize(), true);
        warm(e.ma20(), 20, e.windowSize(), true);
        warm(e.volMa5(), 5, e.windowSize(), true);
        warm(e.volMa10(), 10, e.windowSize(), true);
        decimal(e.volumeRatio(), false, true);
        if (e.volMa5() == null || e.volMa5().signum() == 0) {
            if (e.volumeRatio() != null) bad();
        } else if (e.volumeRatio() == null) bad();
    }

    private static void warm(BigDecimal n, int period, int size, boolean nonnegative) {
        if ((size < period) != (n == null)) bad();
        decimal(n, false, nonnegative);
    }

    private static void decimal(BigDecimal n, boolean required, boolean nonnegative) {
        if (n == null) {
            if (required) bad();
            return;
        }
        if (n.scale() > 6 || n.precision() - n.scale() > 32 || (nonnegative && n.signum() < 0))
            bad();
    }

    private static void text(String s) {
        if (s == null || s.isBlank()) bad();
    }

    private static void bad() {
        throw new IllegalArgumentException("Invalid indicator event");
    }
}
