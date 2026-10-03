package com.stock.flink.kafka;

import com.stock.flink.model.RawKafkaRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public final class RawKafkaRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<RawKafkaRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawKafkaRecord> out) {
        out.collect(new RawKafkaRecord(
                record.topic(), record.partition(), record.offset(), record.timestamp(),
                utf8OrNull(record.key()), utf8OrNull(record.value())));
    }

    @Override
    public TypeInformation<RawKafkaRecord> getProducedType() {
        return TypeInformation.of(RawKafkaRecord.class);
    }

    private static String utf8OrNull(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
