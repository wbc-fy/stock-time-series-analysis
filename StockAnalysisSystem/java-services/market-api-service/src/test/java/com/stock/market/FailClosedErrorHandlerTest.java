package com.stock.market;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.stock.market.ingest.FailClosedErrorHandler;

import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

class FailClosedErrorHandlerTest {
    @Test
    void retriesWithoutCommitThenStopsInsteadOfRecoveringPoison() {
        var stop = mock(CommonErrorHandler.class);
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);
        var handler = new FailClosedErrorHandler(new FixedBackOff(0, 2), stop);
        List<ConsumerRecord<?, ?>> records =
                List.of(new ConsumerRecord<>("topic", 0, 4, "key", "secret poison"));
        var failure = new IllegalArgumentException("Invalid indicator event");
        for (int i = 0; i < 2; i++) {
            assertThatThrownBy(() -> handler.handleRemaining(failure, records, consumer, container))
                    .isInstanceOf(RuntimeException.class);
            verifyNoInteractions(stop);
        }
        handler.handleRemaining(failure, records, consumer, container);
        verify(stop).handleRemaining(any(), eq(records), eq(consumer), eq(container));
        verify(consumer, never()).commitSync(anyMap());
        assertThat(handler.isAckAfterHandle()).isFalse();
    }
}
