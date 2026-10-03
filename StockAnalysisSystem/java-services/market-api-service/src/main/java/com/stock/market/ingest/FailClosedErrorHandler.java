package com.stock.market.ingest;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.BackOff;

import java.util.*;

/** Never treats an exhausted poison record as recovered or eligible for commit. */
public class FailClosedErrorHandler implements CommonErrorHandler {
    private final ThreadLocal<Boolean> exhausted = ThreadLocal.withInitial(() -> false);
    private final DefaultErrorHandler retry;
    private final CommonErrorHandler stop;

    public FailClosedErrorHandler(BackOff backOff, CommonErrorHandler stop) {
        this.stop = stop;
        retry =
                new DefaultErrorHandler(
                        (record, error) -> {
                            exhausted.set(true);
                            throw new IllegalStateException("Indicator retries exhausted");
                        },
                        backOff);
        retry.setAckAfterHandle(false);
        retry.setCommitRecovered(false);
        retry.setClassifications(Map.of(Exception.class, true), true);
    }

    @Override
    public boolean seeksAfterHandling() {
        return true;
    }

    @Override
    public boolean isAckAfterHandle() {
        return false;
    }

    @Override
    public void handleRemaining(
            Exception error,
            List<ConsumerRecord<?, ?>> records,
            Consumer<?, ?> consumer,
            MessageListenerContainer container) {
        exhausted.set(false);
        try {
            retry.handleRemaining(error, records, consumer, container);
        } catch (Exception pendingOrExhausted) {
            if (!exhausted.get()) throw pendingOrExhausted;
            LoggerFactory.getLogger(getClass())
                    .error(
                            "Indicator consumer stopped after bounded retries; poison offset"
                                    + " remains unacknowledged");
            stop.handleRemaining(
                    new IllegalStateException("Indicator persistence failed"),
                    records,
                    consumer,
                    container);
        } finally {
            exhausted.remove();
        }
    }

    @Override
    public void handleOtherException(
            Exception error,
            Consumer<?, ?> consumer,
            MessageListenerContainer container,
            boolean batch) {
        stop.handleOtherException(
                new IllegalStateException("Indicator consumer failed"), consumer, container, batch);
    }
}
