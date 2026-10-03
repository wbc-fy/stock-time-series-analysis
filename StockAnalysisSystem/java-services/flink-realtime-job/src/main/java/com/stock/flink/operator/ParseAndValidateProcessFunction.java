package com.stock.flink.operator;

import com.stock.common.model.FlinkDeadLetterEvent;
import com.stock.flink.model.RawKafkaRecord;
import com.stock.flink.model.ValidatedStockDailyEvent;
import com.stock.flink.validation.InvalidStockDailyEventException;
import com.stock.flink.validation.StockDailyEventParser;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

public final class ParseAndValidateProcessFunction
        extends ProcessFunction<RawKafkaRecord, ValidatedStockDailyEvent> {
    private final int supportedSchemaVersion;
    private final ProcessingClock clock;
    private transient StockDailyEventParser parser;

    public ParseAndValidateProcessFunction(int supportedSchemaVersion, ProcessingClock clock) {
        this.supportedSchemaVersion = supportedSchemaVersion;
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public void open(OpenContext context) {
        parser = new StockDailyEventParser(supportedSchemaVersion);
    }

    @Override
    public void processElement(RawKafkaRecord raw, Context ctx, Collector<ValidatedStockDailyEvent> out) {
        try {
            out.collect(parser.parse(raw));
        } catch (InvalidStockDailyEventException error) {
            ctx.output(FlinkOutputTags.DEAD_LETTER, new FlinkDeadLetterEvent(
                    raw.topic(), raw.partition(), raw.offset(), raw.key(), raw.payload(),
                    error.errorType(), error.getMessage(), clock.now(), 1));
        }
    }
}
