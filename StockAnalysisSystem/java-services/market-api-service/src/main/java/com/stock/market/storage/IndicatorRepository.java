package com.stock.market.storage;

import com.fasterxml.jackson.databind.*;
import com.stock.common.model.StockDailyIndicatorEvent;
import com.stock.market.api.QueryParameters;
import com.stock.market.ingest.IndicatorListener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.*;

@Repository
public class IndicatorRepository {
    private final ClickHouseClient client;
    private final ObjectMapper json =
            IndicatorListener.mapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    public IndicatorRepository(ClickHouseClient client) {
        this.client = client;
    }

    public void insert(StockDailyIndicatorEvent event, ConsumerRecord<String, String> record) {
        try {
            Map<String, Object> row = new LinkedHashMap<>();
            for (var component : StockDailyIndicatorEvent.class.getRecordComponents()) {
                String name =
                        component
                                .getName()
                                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                                .toLowerCase(Locale.ROOT);
                Object value = component.getAccessor().invoke(event);
                if (value instanceof java.time.OffsetDateTime time)
                    value = time.toInstant().toString();
                row.put(name, value);
            }
            row.put("kafka_topic", record.topic());
            row.put("kafka_partition", record.partition());
            row.put("kafka_offset", record.offset());
            row.put("version", record.offset());
            String response =
                    client.execute(
                            "INSERT INTO daily_indicators FORMAT JSONEachRow",
                            Map.of(),
                            json.writeValueAsString(row) + "\n");
            if (!response.isBlank()) throw new DependencyException();
        } catch (DependencyException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DependencyException();
        }
    }

    public List<Map<String, Object>> query(QueryParameters p) {
        var params = new LinkedHashMap<String, String>();
        params.put("code", p.code());
        params.put("limit", Integer.toString(p.limit()));
        String filters = "";
        if (p.start() != null) {
            filters += " AND trade_date >= {start:Date}";
            params.put("start", p.start().toString());
        }
        if (p.end() != null) {
            filters += " AND trade_date <= {end:Date}";
            params.put("end", p.end().toString());
        }
        String sql =
                "SELECT * FROM (SELECT"
                    + " trade_date,close,volume,pct_chg,ma5,ma10,ma20,vol_ma5,vol_ma10,volume_ratio,window_size,is_warmup,calculation_time,event_id,source_event_id,trace_id,source"
                    + " FROM daily_indicators FINAL WHERE ts_code = {code:String}"
                        + filters
                        + " ORDER BY trade_date DESC LIMIT {limit:UInt32}) ORDER BY trade_date ASC"
                        + " FORMAT JSONEachRow";
        try {
            List<Map<String, Object>> result = new ArrayList<>();
            for (String line :
                    client.execute(sql, params, "").lines().filter(s -> !s.isBlank()).toList()) {
                JsonNode node = json.readTree(line);
                Map<String, Object> row = new LinkedHashMap<>();
                node.fields()
                        .forEachRemaining(
                                entry -> {
                                    JsonNode n = entry.getValue();
                                    String key = entry.getKey();
                                    Object value =
                                            n.isNull()
                                                    ? null
                                                    : n.isBoolean()
                                                            ? n.booleanValue()
                                                            : n.isNumber()
                                                                    ? n.decimalValue()
                                                                    : n.asText();
                                    if (value != null
                                            && Set.of(
                                                            "close",
                                                            "volume",
                                                            "pct_chg",
                                                            "ma5",
                                                            "ma10",
                                                            "ma20",
                                                            "vol_ma5",
                                                            "vol_ma10",
                                                            "volume_ratio")
                                                    .contains(key))
                                        value = new BigDecimal(n.asText());
                                    if (key.equals("is_warmup") && !n.isNull())
                                        value = n.isBoolean() ? n.booleanValue() : n.asInt() != 0;
                                    if (key.equals("calculation_time") && value instanceof String s)
                                        value = s.contains("T") ? s : s.replace(' ', 'T') + "Z";
                                    row.put(key, value);
                                });
                result.add(row);
            }
            return result;
        } catch (Exception ex) {
            throw new DependencyException();
        }
    }
}
