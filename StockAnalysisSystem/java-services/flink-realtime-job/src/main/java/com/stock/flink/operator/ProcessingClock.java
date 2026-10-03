package com.stock.flink.operator;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.ZoneId;

@FunctionalInterface
public interface ProcessingClock extends Serializable {
    OffsetDateTime now();

    static ProcessingClock systemShanghai() {
        return () -> OffsetDateTime.now(ZoneId.of("Asia/Shanghai"));
    }
}
