package com.stock.market;

import static org.assertj.core.api.Assertions.*;

import com.stock.market.storage.*;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Test;

import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

class ClickHouseClientTest {
    @Test
    void boundsCompleteResponseWhenHeadersAndPartialBodyArriveBeforeStall() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                x -> {
                    try {
                        x.getRequestBody().readAllBytes();
                        x.sendResponseHeaders(200, 2);
                        x.getResponseBody().write('1');
                        x.getResponseBody().flush();
                        Thread.sleep(1200);
                        x.getResponseBody().write('2');
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        x.close();
                    }
                });
        server.start();
        try {
            var client =
                    new ClickHouseClient(
                            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                            "db",
                            "user",
                            "secret",
                            Duration.ofMillis(200));
            long started = System.nanoTime();
            assertThatThrownBy(() -> client.execute("SELECT 1", Map.of(), ""))
                    .isInstanceOf(DependencyException.class)
                    .hasMessage("Market storage unavailable");
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(Duration.ofMillis(900));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void boundsSlowDependencyWithRequestTimeout() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                x -> {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    x.close();
                });
        server.start();
        try {
            var client =
                    new ClickHouseClient(
                            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                            "db",
                            "user",
                            "secret",
                            Duration.ofMillis(50));
            assertThatThrownBy(() -> client.execute("SELECT 1", Map.of(), ""))
                    .isInstanceOf(DependencyException.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void authenticatesEscapesParametersAndRejectsHttpAndBodyErrors() throws Exception {
        var request = new AtomicReference<String>();
        var auth = new AtomicReference<String>();
        var body = new AtomicReference<String>("");
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                x -> {
                    request.set(x.getRequestURI().getRawQuery());
                    auth.set(x.getRequestHeaders().getFirst("Authorization"));
                    x.getRequestBody().readAllBytes();
                    var bytes = body.get().getBytes();
                    x.sendResponseHeaders(
                            body.get().equals("http") ? 503 : 200,
                            bytes.length == 0 ? -1 : bytes.length);
                    if (bytes.length > 0) x.getResponseBody().write(bytes);
                    x.close();
                });
        server.start();
        try {
            var client =
                    new ClickHouseClient(
                            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                            "stock_analytics",
                            "stock_app",
                            "secret",
                            Duration.ofSeconds(1));
            client.execute("SELECT {code:String}", Map.of("code", "a&b'"), "");
            assertThat(request.get()).contains("wait_end_of_query=1", "param_code=a%26b%27");
            assertThat(auth.get())
                    .isEqualTo(
                            "Basic "
                                    + Base64.getEncoder()
                                            .encodeToString("stock_app:secret".getBytes()));
            body.set("actual complete response");
            assertThat(client.execute("SELECT 1", Map.of(), ""))
                    .isEqualTo("actual complete response");
            body.set("Code: 27. DB::Exception: secret internal");
            assertThatThrownBy(() -> client.execute("SELECT 1", Map.of(), ""))
                    .isInstanceOf(DependencyException.class)
                    .hasMessage("Market storage unavailable");
            body.set("http");
            assertThatThrownBy(() -> client.execute("SELECT 1", Map.of(), ""))
                    .isInstanceOf(DependencyException.class);
        } finally {
            server.stop(0);
        }
    }
}
