package com.stock.flink.validation;

public final class InvalidStockDailyEventException extends RuntimeException {
    private final String errorType;

    public InvalidStockDailyEventException(String errorType, String message) {
        super(message);
        this.errorType = errorType;
    }

    public InvalidStockDailyEventException(String errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
    }

    public String errorType() {
        return errorType;
    }
}
