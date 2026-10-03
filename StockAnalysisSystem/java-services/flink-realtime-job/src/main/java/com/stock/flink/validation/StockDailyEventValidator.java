package com.stock.flink.validation;

import com.stock.common.model.StockDailyEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class StockDailyEventValidator {
    public List<String> violations(String kafkaKey, StockDailyEvent event, int supportedSchemaVersion) {
        List<String> errors = new ArrayList<>();
        requireText(event.eventId(), "eventId", errors);
        requireText(event.traceId(), "traceId", errors);
        requireText(event.tsCode(), "tsCode", errors);
        requireText(event.source(), "source", errors);
        requireValue(event.tradeDate(), "tradeDate", errors);
        requireValue(event.open(), "open", errors);
        requireValue(event.high(), "high", errors);
        requireValue(event.low(), "low", errors);
        requireValue(event.close(), "close", errors);
        requireValue(event.preClose(), "preClose", errors);
        requireValue(event.volume(), "volume", errors);
        requireValue(event.amount(), "amount", errors);
        requireValue(event.eventTime(), "eventTime", errors);
        requireValue(event.ingestTime(), "ingestTime", errors);
        if (!Objects.equals(kafkaKey, event.tsCode())) {
            errors.add("kafkaKey must equal tsCode");
        }
        if (!Objects.equals(event.schemaVersion(), supportedSchemaVersion)) {
            errors.add("unsupported schemaVersion");
        }
        if (event.close() != null && event.close().signum() < 0) {
            errors.add("close must be greater than or equal to zero");
        }
        if (event.volume() != null && event.volume().signum() < 0) {
            errors.add("volume must be non-negative");
        }
        if (event.amount() != null && event.amount().signum() < 0) {
            errors.add("amount must be non-negative");
        }
        return errors;
    }

    private static void requireText(String value, String name, List<String> errors) {
        if (value == null || value.isBlank()) {
            errors.add(name + " must not be blank");
        }
    }

    private static void requireValue(Object value, String name, List<String> errors) {
        if (value == null) {
            errors.add(name + " must not be null");
        }
    }
}
