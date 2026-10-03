package com.stock.market;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.stock.market.api.*;
import com.stock.market.storage.*;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

class MarketControllerTest {
    @Test
    void emitsNumericPricesIsoDatesAndNullableOptionals() throws Exception {
        var mysql = mock(MySqlMarketRepository.class);
        var indicators = mock(IndicatorRepository.class);
        when(mysql.exists("000001.SZ")).thenReturn(true);
        var row = new java.util.LinkedHashMap<String, Object>();
        row.put("trade_date", "2026-09-01");
        row.put("close", new java.math.BigDecimal("10.000000"));
        row.put("pct_chg", null);
        when(mysql.bars(any())).thenReturn(List.of(row));
        when(indicators.query(any())).thenReturn(List.of(row));
        var mvc =
                MockMvcBuilders.standaloneSetup(new MarketController(mysql, indicators))
                        .setControllerAdvice(new ApiExceptionHandler())
                        .build();
        mvc.perform(get("/api/analysis/kline/000001.SZ"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bars[0].close").isNumber())
                .andExpect(jsonPath("$.bars[0].trade_date").value("2026-09-01"))
                .andExpect(jsonPath("$.bars[0].pct_chg").value(org.hamcrest.Matchers.nullValue()));
        mvc.perform(get("/api/analysis/indicators/000001.SZ"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.indicators[0].close").isNumber());
        when(indicators.query(any())).thenThrow(new DependencyException());
        mvc.perform(get("/api/analysis/indicators/000001.SZ"))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void validatesParametersDistinguishesMissingEmptyAndUnavailable() throws Exception {
        var mysql = mock(MySqlMarketRepository.class);
        var indicators = mock(IndicatorRepository.class);
        var mvc =
                MockMvcBuilders.standaloneSetup(new MarketController(mysql, indicators))
                        .setControllerAdvice(new ApiExceptionHandler())
                        .build();
        mvc.perform(get("/api/analysis/kline/bad")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/analysis/stocks?limit=6001")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/analysis/indicators/000001.SZ")).andExpect(status().isNotFound());
        when(mysql.exists("000001.SZ")).thenReturn(true);
        when(indicators.query(any())).thenReturn(List.of());
        mvc.perform(get("/api/analysis/indicators/000001.SZ"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.indicators").isEmpty())
                .andExpect(jsonPath("$.ts_code").value("000001.SZ"));
        mvc.perform(get("/api/analysis/kline/000001.SZ?start=2026-02-01&end=2026-01-01"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/analysis/kline/000001.SZ?start=garbage"))
                .andExpect(status().isBadRequest());
        when(mysql.stocks(6000)).thenThrow(new DependencyException());
        mvc.perform(get("/api/analysis/stocks"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Market dependency unavailable"));
    }
}
