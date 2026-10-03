package com.stock.market.api;

import com.stock.market.storage.*;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.util.*;

@RestController
@RequestMapping("/api/analysis")
public class MarketController {
    private final MySqlMarketRepository mysql;
    private final IndicatorRepository indicators;

    public MarketController(MySqlMarketRepository mysql, IndicatorRepository indicators) {
        this.mysql = mysql;
        this.indicators = indicators;
    }

    @GetMapping("/stocks")
    public List<Map<String, Object>> stocks(@RequestParam(defaultValue = "6000") int limit) {
        return mysql.stocks(QueryParameters.bounds(limit, 6000));
    }

    @GetMapping("/kline/{tsCode}")
    public Map<String, Object> kline(
            @PathVariable String tsCode,
            @RequestParam(defaultValue = "260") int limit,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
                    LocalDate start,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
                    LocalDate end) {
        var p = QueryParameters.series(tsCode, limit, start, end);
        known(tsCode);
        return Map.of("ts_code", tsCode, "bars", mysql.bars(p));
    }

    @GetMapping("/indicators/{tsCode}")
    public Map<String, Object> indicators(
            @PathVariable String tsCode,
            @RequestParam(defaultValue = "260") int limit,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
                    LocalDate start,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
                    LocalDate end) {
        var p = QueryParameters.series(tsCode, limit, start, end);
        known(tsCode);
        return Map.of("ts_code", tsCode, "indicators", indicators.query(p));
    }

    private void known(String code) {
        if (!mysql.exists(code))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stock not found");
    }
}
