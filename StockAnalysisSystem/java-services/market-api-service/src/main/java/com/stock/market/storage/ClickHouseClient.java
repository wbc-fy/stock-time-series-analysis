package com.stock.market.storage;

import java.net.*;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class ClickHouseClient {
    private final URI endpoint;
    private final String database, authorization;
    private final Duration timeout;
    private final HttpClient client;

    public ClickHouseClient(
            URI endpoint, String database, String user, String password, Duration timeout) {
        if (password == null || password.isBlank())
            throw new IllegalArgumentException("CLICKHOUSE_PASSWORD is required");
        if (endpoint.getUserInfo() != null
                || endpoint.getQuery() != null
                || endpoint.getFragment() != null
                || !Set.of("http", "https").contains(endpoint.getScheme()))
            throw new IllegalArgumentException("Invalid ClickHouse endpoint");
        this.endpoint = endpoint;
        this.database = database;
        this.timeout = timeout;
        this.authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (user + ":" + password).getBytes(StandardCharsets.UTF_8));
        this.client =
                HttpClient.newBuilder()
                        .connectTimeout(timeout)
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build();
    }

    public String execute(String sql, Map<String, String> parameters, String data) {
        StringBuilder query =
                new StringBuilder("wait_end_of_query=1&database=")
                        .append(encode(database))
                        .append("&query=")
                        .append(encode(sql));
        parameters.forEach(
                (key, value) ->
                        query.append("&param_")
                                .append(encode(key))
                                .append("=")
                                .append(encode(value)));
        query.append(
                "&output_format_json_quote_64bit_integers=0&date_time_input_format=best_effort");
        try {
            var request =
                    HttpRequest.newBuilder(URI.create(endpoint.toString() + "?" + query))
                            .timeout(timeout)
                            .header("Authorization", authorization)
                            .POST(HttpRequest.BodyPublishers.ofString(data))
                            .build();
            long deadline = System.nanoTime() + timeout.toNanos();
            var response = client.send(request, info -> new DeadlineBodySubscriber(deadline));
            if (response.statusCode() != 200
                    || response.headers()
                            .firstValue("X-ClickHouse-Exception-Code")
                            .filter(v -> !v.equals("0"))
                            .isPresent()
                    || response.body().contains("DB::Exception")
                    || response.body().stripLeading().startsWith("Code:"))
                throw new DependencyException();
            return response.body();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new DependencyException();
        } catch (Exception ex) {
            throw new DependencyException();
        }
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    /** Request timeout covers headers; this subscriber also bounds the complete body. */
    private static final class DeadlineBodySubscriber
            implements HttpResponse.BodySubscriber<String> {
        private final HttpResponse.BodySubscriber<String> delegate =
                HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8);
        private final long deadline;

        private DeadlineBodySubscriber(long deadline) {
            this.deadline = deadline;
        }

        @Override
        public CompletionStage<String> getBody() {
            return delegate.getBody();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            delegate.getBody()
                    .toCompletableFuture()
                    .orTimeout(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
                    .whenComplete(
                            (body, failure) -> {
                                if (failure != null) subscription.cancel();
                            });
            delegate.onSubscribe(subscription);
        }

        @Override
        public void onNext(List<ByteBuffer> item) {
            delegate.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }
}
