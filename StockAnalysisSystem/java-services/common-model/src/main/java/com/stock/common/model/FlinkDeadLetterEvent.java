package com.stock.common.model;

import java.time.OffsetDateTime;

public record FlinkDeadLetterEvent(
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        String originalKey,
        String originalPayload,
        String errorType,
        String errorMessage,
        OffsetDateTime failedAt,
        Integer schemaVersion
) {
}
