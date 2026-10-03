package com.stock.flink;

import com.stock.flink.config.FlinkJobConfig;
import com.stock.flink.operator.FlinkOutputTags;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DailyIndicatorJobTest {
    private static final FlinkJobConfig CONFIG = new FlinkJobConfig(
            "broker:9092", "stock.ods.daily.v1", "stock.dws.daily-indicator.v1",
            "stock.late.daily.v1", "stock.flink.dead-letter.v1", "stock-flink-daily-indicator-v1",
            "file:///tmp/test-flink-checkpoints", 10_000L, 60_000L, 5_000L, 600_000L, 3, 1,
            "team-a-stock-flink");

    @Test
    void shouldConfigureExactlyOnceCheckpointsAndFixedDelayRestart() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DailyIndicatorJob.configure(env, CONFIG);

        assertEquals(3, env.getParallelism());
        assertEquals(10_000L, env.getCheckpointConfig().getCheckpointInterval());
        assertEquals(60_000L, env.getCheckpointConfig().getCheckpointTimeout());
        assertEquals(5_000L, env.getCheckpointConfig().getMinPauseBetweenCheckpoints());
        assertEquals(1, env.getCheckpointConfig().getMaxConcurrentCheckpoints());
        assertEquals(org.apache.flink.core.execution.CheckpointingMode.EXACTLY_ONCE,
                env.getCheckpointConfig().getCheckpointingConsistencyMode());
        assertEquals(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION,
                env.getCheckpointConfig().getExternalizedCheckpointRetention());
        assertEquals(CONFIG.checkpointUri(), env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
        assertEquals("fixed-delay", env.getConfiguration().get(RestartStrategyOptions.RESTART_STRATEGY));
        assertEquals(3, env.getConfiguration().get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS));
        assertEquals(Duration.ofSeconds(10),
                env.getConfiguration().get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY));
    }

    @Test
    void shouldUseConfiguredKafkaSourceAndCommittedOffsetsWithEarliestFallback() throws Exception {
        KafkaSource<?> source = DailyIndicatorJob.source(CONFIG);
        Properties properties = field(source, "props", Properties.class);
        OffsetsInitializer offsets = field(source, "startingOffsetsInitializer", OffsetsInitializer.class);
        Object subscriber = field(source, "subscriber", Object.class);

        assertEquals(CONFIG.bootstrapServers(), properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(CONFIG.consumerGroup(), properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(OffsetResetStrategy.EARLIEST, offsets.getAutoOffsetResetStrategy());
        assertEquals(List.of(CONFIG.inputTopic()), field(subscriber, "topics", List.class));
    }

    @Test
    void shouldMakeAllThreeSinksTransactionalWithDistinctStablePrefixes() throws Exception {
        Map<String, KafkaSink<?>> sinks = Map.of(
                "team-a-stock-flink-indicator-v1-main-", DailyIndicatorJob.mainSink(CONFIG),
                "team-a-stock-flink-indicator-v1-late-", DailyIndicatorJob.lateSink(CONFIG),
                "team-a-stock-flink-indicator-v1-dlt-", DailyIndicatorJob.deadLetterSink(CONFIG));
        Map<String, String> topics = Map.of(
                "team-a-stock-flink-indicator-v1-main-", CONFIG.outputTopic(),
                "team-a-stock-flink-indicator-v1-late-", CONFIG.lateTopic(),
                "team-a-stock-flink-indicator-v1-dlt-", CONFIG.deadLetterTopic());

        for (var entry : sinks.entrySet()) {
            KafkaSink<?> sink = entry.getValue();
            assertEquals(DeliveryGuarantee.EXACTLY_ONCE,
                    field(sink, "deliveryGuarantee", DeliveryGuarantee.class));
            assertEquals(entry.getKey(), field(sink, "transactionalIdPrefix", String.class));
            Properties properties = field(sink, "kafkaProducerConfig", Properties.class);
            assertEquals(CONFIG.bootstrapServers(), properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            assertEquals("600000", properties.getProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG));
            Object serializer = field(sink, "recordSerializer", Object.class);
            assertEquals(topics.get(entry.getKey()), field(serializer, "topic", String.class));
        }
    }

    @Test
    void shouldConnectValidLateAndDeadLetterBranchesWithStableUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DailyIndicatorJob.configure(env, CONFIG);
        DailyIndicatorJob.buildTopology(env, CONFIG);

        StreamGraph graph = env.getStreamGraph();
        Map<String, StreamNode> nodes = graph.getStreamNodes().stream()
                .filter(node -> node.getTransformationUID() != null)
                .collect(Collectors.toMap(StreamNode::getTransformationUID, Function.identity()));
        Set<String> requiredUids = Set.of("daily-kafka-source-v1", "daily-parse-v1",
                "daily-indicator-state-v1", "daily-main-sink-v1", "daily-late-sink-v1",
                "daily-dlt-sink-v1");
        assertTrue(nodes.keySet().containsAll(requiredUids));

        StreamNode source = nodes.get("daily-kafka-source-v1");
        StreamNode parse = nodes.get("daily-parse-v1");
        StreamNode indicator = nodes.get("daily-indicator-state-v1");
        assertTrue(hasEdge(source, parse, null));
        assertTrue(hasEdge(parse, indicator, null));
        assertFalse(hasEdge(parse, indicator, FlinkOutputTags.DEAD_LETTER.getId()));
        assertTrue(hasEdge(indicator, nodes.get("daily-main-sink-v1"), null));
        assertTrue(hasEdge(indicator, nodes.get("daily-late-sink-v1"), FlinkOutputTags.LATE.getId()));
        assertTrue(hasEdge(parse, nodes.get("daily-dlt-sink-v1"), FlinkOutputTags.DEAD_LETTER.getId()));
    }

    private static boolean hasEdge(StreamNode source, StreamNode target, String tagId) {
        return source.getOutEdges().stream()
                .anyMatch(edge -> edge.getTargetId() == target.getId() && hasTag(edge, tagId));
    }

    private static boolean hasTag(StreamEdge edge, String tagId) {
        return tagId == null ? edge.getOutputTag() == null
                : edge.getOutputTag() != null && tagId.equals(edge.getOutputTag().getId());
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Object object, String name, Class<T> type) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (T) type.cast(field.get(object));
    }
}
