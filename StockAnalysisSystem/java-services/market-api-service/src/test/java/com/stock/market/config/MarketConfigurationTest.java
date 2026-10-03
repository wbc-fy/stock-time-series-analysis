package com.stock.market.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.*;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;

class MarketConfigurationTest {
    @Test
    void blankOrMissingPasswordsFailWithoutEchoingCredentials() {
        var properties = new MarketProperties();
        var env = new MockEnvironment();
        assertThatThrownBy(() -> properties.clickHouseClient(env))
                .hasMessage("DB_PASSWORD is required");
        env.withProperty("DB_PASSWORD", "private-db");
        assertThatThrownBy(() -> properties.clickHouseClient(env))
                .hasMessage("CLICKHOUSE_PASSWORD is required");
        env.withProperty("CLICKHOUSE_PASSWORD", " ");
        assertThatThrownBy(() -> properties.clickHouseClient(env))
                .hasMessage("CLICKHOUSE_PASSWORD is required");
    }

    @Test
    void unhealthyChildMakesConsumerHealthDownEvenWhenParentIsRunning() {
        var registry = mock(KafkaListenerEndpointRegistry.class);
        var parent = mock(ConcurrentMessageListenerContainer.class);
        var child = mock(KafkaMessageListenerContainer.class);
        when(registry.getListenerContainer("marketIndicators")).thenReturn(parent);
        when(parent.isRunning()).thenReturn(true);
        when(parent.getContainers()).thenReturn(List.of(child));
        var configuration = new KafkaConfiguration();
        assertThat(configuration.indicatorConsumerHealth(registry, true).health().getStatus())
                .isEqualTo(Status.DOWN);
        when(child.isRunning()).thenReturn(true);
        assertThat(configuration.indicatorConsumerHealth(registry, true).health().getStatus())
                .isEqualTo(Status.UP);
        when(child.isRunning()).thenReturn(false);
        assertThat(configuration.indicatorConsumerHealth(registry, false).health().getStatus())
                .isEqualTo(Status.UP);
    }

    @Test
    void kafkaConfigurationUsesReadCommittedManualImmediateAndEarliest() {
        var factory = new KafkaConfiguration().indicatorFactory("localhost:9092", "test-group");
        assertThat(factory.getConsumerFactory().getConfigurationProperties())
                .containsEntry("enable.auto.commit", false)
                .containsEntry("isolation.level", "read_committed")
                .containsEntry("auto.offset.reset", "earliest");
        assertThat(factory.getContainerProperties().getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    }
}
