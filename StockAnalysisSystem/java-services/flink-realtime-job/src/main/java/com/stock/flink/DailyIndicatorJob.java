package com.stock.flink;

import com.stock.common.model.FlinkDeadLetterEvent;
import com.stock.common.model.LateStockDailyEvent;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.flink.config.FlinkJobConfig;
import com.stock.flink.kafka.JsonKafkaRecordSerializationSchema;
import com.stock.flink.kafka.RawKafkaRecordDeserializationSchema;
import com.stock.flink.model.RawKafkaRecord;
import com.stock.flink.model.ValidatedStockDailyEvent;
import com.stock.flink.operator.DailyIndicatorProcessFunction;
import com.stock.flink.operator.FlinkOutputTags;
import com.stock.flink.operator.ParseAndValidateProcessFunction;
import com.stock.flink.operator.ProcessingClock;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Properties;

public final class DailyIndicatorJob {
    private DailyIndicatorJob() {
    }

    public static void main(String[] args) throws Exception {
        FlinkJobConfig config = FlinkJobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, config);
        buildTopology(env, config);
        env.execute("stock-daily-indicator-v1");
    }

    static void configure(StreamExecutionEnvironment env, FlinkJobConfig config) {
        Configuration settings = new Configuration();
        settings.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointUri());
        settings.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        settings.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        settings.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        env.configure(settings);

        env.setParallelism(config.parallelism());
        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.checkpointMinPauseMs());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    static void buildTopology(StreamExecutionEnvironment env, FlinkJobConfig config) {
        DataStream<RawKafkaRecord> raw = env.fromSource(
                        source(config), WatermarkStrategy.noWatermarks(), "stock-daily-kafka-source")
                .uid("daily-kafka-source-v1");

        SingleOutputStreamOperator<ValidatedStockDailyEvent> valid = raw
                .process(new ParseAndValidateProcessFunction(
                        config.supportedSchemaVersion(), ProcessingClock.systemShanghai()))
                .uid("daily-parse-v1");
        DataStream<FlinkDeadLetterEvent> deadLetters = valid.getSideOutput(FlinkOutputTags.DEAD_LETTER);

        SingleOutputStreamOperator<StockDailyIndicatorEvent> indicators = valid
                .keyBy(value -> value.event().tsCode())
                .process(new DailyIndicatorProcessFunction(ProcessingClock.systemShanghai()))
                .uid("daily-indicator-state-v1");
        DataStream<LateStockDailyEvent> late = indicators.getSideOutput(FlinkOutputTags.LATE);

        indicators.sinkTo(mainSink(config)).uid("daily-main-sink-v1");
        late.sinkTo(lateSink(config)).uid("daily-late-sink-v1");
        deadLetters.sinkTo(deadLetterSink(config)).uid("daily-dlt-sink-v1");
    }

    static KafkaSource<RawKafkaRecord> source(FlinkJobConfig config) {
        return KafkaSource.<RawKafkaRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.inputTopic())
                .setGroupId(config.consumerGroup())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new RawKafkaRecordDeserializationSchema())
                .build();
    }

    static KafkaSink<StockDailyIndicatorEvent> mainSink(FlinkJobConfig config) {
        return sink(config, JsonKafkaRecordSerializationSchema.indicators(config.outputTopic()),
                transactionalIdPrefix(config, "main"));
    }

    static KafkaSink<LateStockDailyEvent> lateSink(FlinkJobConfig config) {
        return sink(config, JsonKafkaRecordSerializationSchema.late(config.lateTopic()),
                transactionalIdPrefix(config, "late"));
    }

    static KafkaSink<FlinkDeadLetterEvent> deadLetterSink(FlinkJobConfig config) {
        return sink(config, JsonKafkaRecordSerializationSchema.deadLetters(config.deadLetterTopic()),
                transactionalIdPrefix(config, "dlt"));
    }

    private static String transactionalIdPrefix(FlinkJobConfig config, String sinkName) {
        return config.deploymentNamespace() + "-indicator-v1-" + sinkName + "-";
    }

    private static <T> KafkaSink<T> sink(
            FlinkJobConfig config, JsonKafkaRecordSerializationSchema<T> serializer, String prefix) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                Long.toString(config.kafkaTransactionTimeoutMs()));
        return KafkaSink.<T>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(prefix)
                .setKafkaProducerConfig(properties)
                .build();
    }
}
