package com.stock.market.storage;

import com.stock.market.api.QueryParameters;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class MySqlMarketRepository {
    private final JdbcTemplate jdbc;

    public MySqlMarketRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        jdbc.setQueryTimeout(5);
    }

    public List<Map<String, Object>> stocks(int limit) {
        try {
            return jdbc
                    .queryForList(
                            "SELECT ts_code,symbol,name,area,industry,list_date FROM stock_basic"
                                    + " ORDER BY ts_code ASC LIMIT ?",
                            limit)
                    .stream()
                    .map(MySqlMarketRepository::dates)
                    .toList();
        } catch (org.springframework.dao.DataAccessException ex) {
            throw new DependencyException();
        }
    }

    public boolean exists(String code) {
        try {
            return Boolean.TRUE.equals(
                    jdbc.queryForObject(
                            "SELECT EXISTS(SELECT 1 FROM stock_basic WHERE ts_code = ?)",
                            Boolean.class,
                            code));
        } catch (org.springframework.dao.DataAccessException ex) {
            throw new DependencyException();
        }
    }

    public List<Map<String, Object>> bars(QueryParameters p) {
        List<Object> args = new ArrayList<>();
        args.add(p.code());
        String filters = "";
        if (p.start() != null) {
            filters += " AND trade_date >= ?";
            args.add(p.start());
        }
        if (p.end() != null) {
            filters += " AND trade_date <= ?";
            args.add(p.end());
        }
        args.add(p.limit());
        String sql =
                "SELECT * FROM (SELECT"
                    + " trade_date,open,high,low,close,pre_close,`change`,pct_chg,vol,amount FROM"
                    + " stock_daily WHERE ts_code = ?"
                        + filters
                        + " ORDER BY trade_date DESC LIMIT ?) latest ORDER BY trade_date ASC";
        try {
            return jdbc.queryForList(sql, args.toArray()).stream()
                    .map(MySqlMarketRepository::dates)
                    .toList();
        } catch (org.springframework.dao.DataAccessException ex) {
            throw new DependencyException();
        }
    }

    private static Map<String, Object> dates(Map<String, Object> row) {
        Map<String, Object> copy = new LinkedHashMap<>(row);
        copy.replaceAll(
                (k, v) ->
                        v instanceof java.sql.Date d
                                ? d.toLocalDate().toString()
                                : v instanceof java.time.LocalDate d ? d.toString() : v);
        return copy;
    }
}
