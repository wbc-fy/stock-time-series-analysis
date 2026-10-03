package com.stock.market.config;

import com.stock.market.storage.ClickHouseClient;

import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;

import java.net.URI;
import java.time.Duration;

@Configuration
public class MarketProperties {
    @Bean
    ClickHouseClient clickHouseClient(Environment env) {
        require(env, "DB_PASSWORD");
        return new ClickHouseClient(
                URI.create(env.getProperty("CLICKHOUSE_HTTP_URL", "http://127.0.0.1:8123")),
                env.getProperty("CLICKHOUSE_DB", "stock_analytics"),
                env.getProperty("CLICKHOUSE_USER", "stock_app"),
                require(env, "CLICKHOUSE_PASSWORD"),
                Duration.ofSeconds(5));
    }

    private String require(Environment env, String key) {
        String value = env.getProperty(key);
        if (value == null || value.isBlank())
            throw new IllegalArgumentException(key + " is required");
        return value;
    }
}
