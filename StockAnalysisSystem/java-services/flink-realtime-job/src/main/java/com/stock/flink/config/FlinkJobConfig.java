package com.stock.flink.config;

import org.apache.flink.util.ParameterTool;

public record FlinkJobConfig(
        String bootstrapServers,
        String inputTopic,
        String outputTopic,
        String lateTopic,
        String deadLetterTopic,
        String consumerGroup,
        String checkpointUri,
        long checkpointIntervalMs,
        long checkpointTimeoutMs,
        long checkpointMinPauseMs,
        long kafkaTransactionTimeoutMs,
        int parallelism,
        int supportedSchemaVersion,
        String deploymentNamespace
) {
    private static final long FIXED_RESTART_BUDGET_MS = 3L * 10_000L;

    public static FlinkJobConfig fromArgs(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        long checkpointTimeoutMs = positive(
                parameters.getLong("checkpoint-timeout-ms", 60_000L), "checkpoint-timeout-ms");
        long kafkaTransactionTimeoutMs = positive(
                parameters.getLong("kafka-transaction-timeout-ms", 600_000L),
                "kafka-transaction-timeout-ms");
        if (checkpointTimeoutMs > Long.MAX_VALUE - FIXED_RESTART_BUDGET_MS
                || kafkaTransactionTimeoutMs <= checkpointTimeoutMs + FIXED_RESTART_BUDGET_MS) {
            throw new IllegalArgumentException(
                    "kafka-transaction-timeout-ms must be greater than checkpoint-timeout-ms plus 30000 ms");
        }
        return new FlinkJobConfig(
                value(parameters, "bootstrap-servers", "FLINK_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                value(parameters, "input-topic", "FLINK_INPUT_TOPIC", "stock.ods.daily.v1"),
                value(parameters, "output-topic", "FLINK_OUTPUT_TOPIC", "stock.dws.daily-indicator.v1"),
                value(parameters, "late-topic", "FLINK_LATE_TOPIC", "stock.late.daily.v1"),
                value(parameters, "dead-letter-topic", "FLINK_DLT_TOPIC", "stock.flink.dead-letter.v1"),
                value(parameters, "group-id", "FLINK_CONSUMER_GROUP", "stock-flink-daily-indicator-v1"),
                value(parameters, "checkpoint-uri", "FLINK_CHECKPOINT_URI", "file:///opt/flink/checkpoints"),
                positive(parameters.getLong("checkpoint-interval-ms", 10_000L), "checkpoint-interval-ms"),
                checkpointTimeoutMs,
                positive(parameters.getLong("checkpoint-min-pause-ms", 5_000L), "checkpoint-min-pause-ms"),
                kafkaTransactionTimeoutMs,
                positiveInt(parameters.getInt("parallelism", 3), "parallelism"),
                positiveInt(parameters.getInt("schema-version", 1), "schema-version"),
                nonBlank(value(parameters, "deployment-namespace", "FLINK_DEPLOYMENT_NAMESPACE", "stock-flink"),
                        "deployment-namespace")
        );
    }

    private static String value(ParameterTool parameters, String arg, String env, String fallback) {
        return parameters.has(arg) ? parameters.getRequired(arg) : System.getenv().getOrDefault(env, fallback);
    }

    private static long positive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static int positiveInt(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String nonBlank(String value, String name) {
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
