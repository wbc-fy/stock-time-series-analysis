package com.stock.market.ingest;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.market.storage.IndicatorRepository;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class IndicatorListener {
    private final IndicatorRepository repository;
    private final IndicatorValidator validator;
    private final ObjectMapper json = mapper();

    public IndicatorListener(IndicatorRepository repository, IndicatorValidator validator) {
        this.repository = repository;
        this.validator = validator;
    }

    public static ObjectMapper mapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    @KafkaListener(
            id = "marketIndicators",
            topics = "${market.indicator-topic}",
            groupId = "${market.kafka-group}",
            containerFactory = "indicatorFactory",
            autoStartup = "${market.listener-enabled:true}")
    public void receive(ConsumerRecord<String, String> record, Acknowledgment ack) {
        StockDailyIndicatorEvent event;
        try {
            event = json.readValue(record.value(), StockDailyIndicatorEvent.class);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Malformed indicator event");
        }
        validator.validate(event, record.key(), record.topic());
        if (record.offset() < 0 || record.partition() < 0)
            throw new IllegalArgumentException("Invalid Kafka metadata");
        repository.insert(event, record);
        ack.acknowledge();
    }
}
