package com.stock.market;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.market.api.QueryParameters;
import com.stock.market.ingest.*;
import com.stock.market.storage.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.*;
import java.util.List;

class MarketBehaviorTest {
    static StockDailyIndicatorEvent event() {
        return new StockDailyIndicatorEvent(
                "e",
                "s",
                "t",
                "000001.SZ",
                LocalDate.of(2026, 9, 1),
                "tushare",
                OffsetDateTime.parse("2026-09-01T15:00:00+08:00"),
                new BigDecimal("10.000000"),
                BigDecimal.ZERO,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                1,
                true,
                OffsetDateTime.parse("2026-09-01T16:00:00+08:00"),
                1);
    }

    @Test
    void validatesWarmupAndRejectsWrongKey() {
        var validator = new IndicatorValidator("topic");
        validator.validate(event(), "000001.SZ", "topic");
        assertThatThrownBy(() -> validator.validate(event(), "000002.SZ", "topic"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validator.validate(event(), "000001.SZ", "wrong"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validatesBoundsAndDates() {
        assertThatThrownBy(() -> QueryParameters.series("bad", 260, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryParameters.series("000001.SZ", 2001, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                QueryParameters.series(
                                        "000001.SZ",
                                        260,
                                        LocalDate.of(2026, 2, 1),
                                        LocalDate.of(2026, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writeFailureNeverAcknowledgesAndRetryWritesBeforeAck() throws Exception {
        var repository = mock(IndicatorRepository.class);
        var ack = mock(Acknowledgment.class);
        var mapper = IndicatorListener.mapper();
        var listener = new IndicatorListener(repository, new IndicatorValidator("topic"));
        var record =
                new ConsumerRecord<String, String>(
                        "topic", 0, 12, "000001.SZ", mapper.writeValueAsString(event()));
        doThrow(new DependencyException()).doNothing().when(repository).insert(any(), any());
        assertThatThrownBy(() -> listener.receive(record, ack))
                .isInstanceOf(DependencyException.class);
        verify(ack, never()).acknowledge();
        listener.receive(record, ack);
        var ordered = inOrder(repository, ack);
        ordered.verify(repository, times(2)).insert(any(), any());
        ordered.verify(ack).acknowledge();
    }

    @Test
    void poisonMessageNeverWritesOrAcknowledges() {
        var repository = mock(IndicatorRepository.class);
        var ack = mock(Acknowledgment.class);
        var listener = new IndicatorListener(repository, new IndicatorValidator("topic"));
        assertThatThrownBy(
                        () ->
                                listener.receive(
                                        new ConsumerRecord<>("topic", 0, 1, "000001.SZ", "{}"),
                                        ack))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(repository, ack);
    }

    @Test
    void fractionalSchemaVersionCannotCoerceToSupportedVersion() throws Exception {
        var repository = mock(IndicatorRepository.class);
        var ack = mock(Acknowledgment.class);
        var listener = new IndicatorListener(repository, new IndicatorValidator("topic"));
        String payload =
                IndicatorListener.mapper()
                        .writeValueAsString(event())
                        .replace("\"schemaVersion\":1", "\"schemaVersion\":1.9");
        assertThatThrownBy(
                        () ->
                                listener.receive(
                                        new ConsumerRecord<>("topic", 0, 1, "000001.SZ", payload),
                                        ack))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(repository, ack);
    }

    @Test
    void validatorRejectsPrecisionWarmupAndSchemaViolations() throws Exception {
        var validator = new IndicatorValidator("topic");
        for (String replacement :
                List.of(
                        "\"close\":-1",
                        "\"close\":1.0000001",
                        "\"close\":100000000000000000000000000000000",
                        "\"windowSize\":0",
                        "\"isWarmup\":false",
                        "\"schemaVersion\":2",
                        "\"eventId\":\"\"",
                        "\"ma5\":1")) {
            String field = replacement.substring(0, replacement.indexOf(':'));
            String original = IndicatorListener.mapper().writeValueAsString(event());
            String payload =
                    original.replaceAll(
                            java.util.regex.Pattern.quote(field) + ":[^,}]+", replacement);
            var changed =
                    IndicatorListener.mapper().readValue(payload, StockDailyIndicatorEvent.class);
            assertThatThrownBy(() -> validator.validate(changed, "000001.SZ", "topic"))
                    .as(replacement)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
