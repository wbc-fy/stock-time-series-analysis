package com.stock.flink.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stock.common.model.StockDailyEvent;
import com.stock.flink.model.RawKafkaRecord;
import com.stock.flink.model.ValidatedStockDailyEvent;

import java.util.List;
import java.util.Objects;

public final class StockDailyEventParser {
    private final int supportedSchemaVersion;
    private final StockDailyEventValidator validator = new StockDailyEventValidator();
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);

    public StockDailyEventParser(int supportedSchemaVersion) {
        this.supportedSchemaVersion = supportedSchemaVersion;
    }

    public ValidatedStockDailyEvent parse(RawKafkaRecord raw) {
        if (raw.payload() == null) {
            throw new InvalidStockDailyEventException("JSON_PARSE", "invalid StockDailyEvent JSON");
        }
        final StockDailyEvent event;
        try {
            event = mapper.readValue(raw.payload(), StockDailyEvent.class);
        } catch (JsonProcessingException e) {
            throw new InvalidStockDailyEventException("JSON_PARSE", "invalid StockDailyEvent JSON", e);
        }
        if (event == null) {
            throw new InvalidStockDailyEventException("JSON_PARSE", "invalid StockDailyEvent JSON");
        }
        List<String> violations = validator.violations(raw.key(), event, supportedSchemaVersion);
        if (!violations.isEmpty()) {
            String errorType = Objects.equals(raw.key(), event.tsCode()) ? "VALIDATION" : "KEY_MISMATCH";
            throw new InvalidStockDailyEventException(errorType, String.join("; ", violations));
        }
        return new ValidatedStockDailyEvent(raw, event);
    }
}
