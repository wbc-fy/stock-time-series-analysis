package com.stock.market.storage;

public class DependencyException extends RuntimeException {
    public DependencyException() {
        super("Market storage unavailable");
    }
}
