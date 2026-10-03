package com.stock.market.config;

import com.stock.market.ingest.FailClosedErrorHandler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.*;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.*;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

import java.util.*;

@Configuration
public class KafkaConfiguration {
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> indicatorFactory(
            @Value("${spring.kafka.bootstrap-servers}") String servers,
            @Value("${market.kafka-group}") String group) {
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(
                new DefaultKafkaConsumerFactory<>(
                        props, new StringDeserializer(), new StringDeserializer()));
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);
        factory.setCommonErrorHandler(
                new FailClosedErrorHandler(
                        new FixedBackOff(1000L, 3L), new CommonContainerStoppingErrorHandler()));
        return factory;
    }

    @Bean
    HealthIndicator indicatorConsumerHealth(
            KafkaListenerEndpointRegistry registry,
            @Value("${market.listener-enabled:true}") boolean enabled) {
        return () -> {
            var c = registry.getListenerContainer("marketIndicators");
            boolean running =
                    c != null
                            && c.isRunning()
                            && (!(c instanceof ConcurrentMessageListenerContainer<?, ?> concurrent)
                                    || (!concurrent.getContainers().isEmpty()
                                            && concurrent.getContainers().stream()
                                                    .allMatch(
                                                            MessageListenerContainer::isRunning)));
            return !enabled
                    ? Health.up().withDetail("consumer", "disabled").build()
                    : running
                            ? Health.up().build()
                            : Health.down().withDetail("consumer", "stopped").build();
        };
    }
}
