package com.stock.flink.operator;

import com.stock.common.model.FlinkDeadLetterEvent;
import com.stock.common.model.LateStockDailyEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public final class FlinkOutputTags {
    public static final OutputTag<LateStockDailyEvent> LATE =
            new OutputTag<>("late-daily", TypeInformation.of(LateStockDailyEvent.class));
    public static final OutputTag<FlinkDeadLetterEvent> DEAD_LETTER =
            new OutputTag<>("flink-dead-letter", TypeInformation.of(FlinkDeadLetterEvent.class));

    private FlinkOutputTags() {
    }
}
