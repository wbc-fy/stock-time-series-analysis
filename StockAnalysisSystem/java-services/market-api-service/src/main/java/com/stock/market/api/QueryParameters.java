package com.stock.market.api;

import java.time.LocalDate;

public record QueryParameters(String code, int limit, LocalDate start, LocalDate end) {
    public static QueryParameters series(String code, int limit, LocalDate start, LocalDate end) {
        if (code == null || !code.matches("[0-9]{6}\\.(SH|SZ|BJ)"))
            throw new IllegalArgumentException("Invalid stock code");
        bounds(limit, 2000);
        if (start != null && end != null && start.isAfter(end))
            throw new IllegalArgumentException("Invalid date range");
        return new QueryParameters(code, limit, start, end);
    }

    public static int bounds(int limit, int max) {
        if (limit < 1 || limit > max) throw new IllegalArgumentException("Invalid limit");
        return limit;
    }
}
