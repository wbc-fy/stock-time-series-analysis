package com.stock.flink.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkJobConfigTest {

    @Test
    void shouldUseDockerDefaults() {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(new String[0]);

        assertEquals("kafka:29092", config.bootstrapServers());
        assertEquals("stock.ods.daily.v1", config.inputTopic());
        assertEquals("stock.dws.daily-indicator.v1", config.outputTopic());
        assertEquals("stock.late.daily.v1", config.lateTopic());
        assertEquals("stock.flink.dead-letter.v1", config.deadLetterTopic());
        assertEquals("stock-flink-daily-indicator-v1", config.consumerGroup());
        assertEquals("file:///opt/flink/checkpoints", config.checkpointUri());
        assertEquals(10_000L, config.checkpointIntervalMs());
        assertEquals(60_000L, config.checkpointTimeoutMs());
        assertEquals(5_000L, config.checkpointMinPauseMs());
        assertEquals(600_000L, config.kafkaTransactionTimeoutMs());
        assertEquals(3, config.parallelism());
        assertEquals(1, config.supportedSchemaVersion());
        assertEquals("stock-flink", config.deploymentNamespace());
    }

    @Test
    void shouldPreferCommandLineStringSettings() {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(new String[] {
                "--bootstrap-servers", "localhost:9092",
                "--input-topic", "input",
                "--output-topic", "output",
                "--late-topic", "late",
                "--dead-letter-topic", "dead-letter",
                "--group-id", "test-group",
                "--checkpoint-uri", "file:///tmp/checkpoints",
                "--deployment-namespace", "team-a-stock-flink"
        });

        assertEquals("localhost:9092", config.bootstrapServers());
        assertEquals("input", config.inputTopic());
        assertEquals("output", config.outputTopic());
        assertEquals("late", config.lateTopic());
        assertEquals("dead-letter", config.deadLetterTopic());
        assertEquals("test-group", config.consumerGroup());
        assertEquals("file:///tmp/checkpoints", config.checkpointUri());
        assertEquals("team-a-stock-flink", config.deploymentNamespace());
    }

    @Test
    void shouldAcceptPositiveNumericOverrides() {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(new String[] {
                "--checkpoint-interval-ms", "1000",
                "--checkpoint-timeout-ms", "2000",
                "--checkpoint-min-pause-ms", "3000",
                "--kafka-transaction-timeout-ms", "32001",
                "--parallelism", "2",
                "--schema-version", "1"
        });

        assertEquals(1000L, config.checkpointIntervalMs());
        assertEquals(2000L, config.checkpointTimeoutMs());
        assertEquals(3000L, config.checkpointMinPauseMs());
        assertEquals(32001L, config.kafkaTransactionTimeoutMs());
        assertEquals(2, config.parallelism());
    }

    @Test
    void shouldRejectNonPositiveNumericSettings() {
        String[] options = {
                "checkpoint-interval-ms", "checkpoint-timeout-ms", "checkpoint-min-pause-ms",
                "kafka-transaction-timeout-ms", "parallelism", "schema-version"
        };
        for (String option : options) {
            assertThrows(IllegalArgumentException.class,
                    () -> FlinkJobConfig.fromArgs(new String[] {"--" + option, "0"}), option);
        }
    }

    @Test
    void shouldRejectTransactionTimeoutEqualToCheckpointAndRestartBudget() {
        assertThrows(IllegalArgumentException.class, () -> FlinkJobConfig.fromArgs(new String[] {
                "--checkpoint-timeout-ms", "60000",
                "--kafka-transaction-timeout-ms", "90000"
        }));
    }

    @Test
    void shouldAcceptTransactionTimeoutGreaterThanCheckpointAndRestartBudget() {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(new String[] {
                "--checkpoint-timeout-ms", "60000",
                "--kafka-transaction-timeout-ms", "90001"
        });

        assertEquals(90001L, config.kafkaTransactionTimeoutMs());
    }

    @Test
    void shouldRejectBlankDeploymentNamespace() {
        assertThrows(IllegalArgumentException.class,
                () -> FlinkJobConfig.fromArgs(new String[] {"--deployment-namespace", " "}));
    }
}
