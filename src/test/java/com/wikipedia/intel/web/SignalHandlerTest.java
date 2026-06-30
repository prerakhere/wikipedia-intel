package com.wikipedia.intel.web;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SignalHandler — validates ring buffer behavior and HTTP routing.
 */
class SignalHandlerTest {

    private SignalFormatter formatter;
    private SignalHandler handler;

    @BeforeEach
    void setUp() {
        formatter = new SignalFormatter();
        handler = new SignalHandler(formatter);
    }

    @Test
    void addSignal_storesSignalInBuffer() {
        TrendingSignal signal = new TrendingSignal("Java", 10, 1000L, 2000L);
        handler.addSignal(signal);

        List<Signal> signals = handler.recentSignals();
        assertEquals(1, signals.size());
        assertEquals(signal, signals.get(0));
    }

    @Test
    void ringBuffer_capsAt50Signals_oldestEvicted() {
        // Add 55 signals — oldest 5 should be evicted
        for (int i = 0; i < 55; i++) {
            handler.addSignal(new TrendingSignal("Article-" + i, i, 1000L * i, 2000L * i));
        }

        List<Signal> signals = handler.recentSignals();
        assertEquals(50, signals.size());

        // Newest (Article-54) should be first
        TrendingSignal newest = (TrendingSignal) signals.get(0);
        assertEquals("Article-54", newest.title());

        // Oldest remaining (Article-5) should be last
        TrendingSignal oldest = (TrendingSignal) signals.get(49);
        assertEquals("Article-5", oldest.title());
    }

    @Test
    void recentSignals_returnsNewestFirst() {
        handler.addSignal(new TrendingSignal("First", 1, 1000L, 2000L));
        handler.addSignal(new TrendingSignal("Second", 2, 3000L, 4000L));
        handler.addSignal(new TrendingSignal("Third", 3, 5000L, 6000L));

        List<Signal> signals = handler.recentSignals();
        assertEquals(3, signals.size());
        assertEquals("Third", ((TrendingSignal) signals.get(0)).title());
        assertEquals("Second", ((TrendingSignal) signals.get(1)).title());
        assertEquals("First", ((TrendingSignal) signals.get(2)).title());
    }

    @Test
    void threadSafety_concurrentAddSignalCallsDontCorruptState() throws InterruptedException {
        int threadCount = 10;
        int signalsPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < signalsPerThread; i++) {
                        handler.addSignal(new TrendingSignal(
                                "T" + threadId + "-" + i, i, 1000L, 2000L));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        List<Signal> signals = handler.recentSignals();
        // Should have exactly 50 (capped) and no corruption
        assertEquals(50, signals.size());
        // Every signal should be non-null and a valid TrendingSignal
        for (Signal s : signals) {
            assertNotNull(s);
            assertInstanceOf(TrendingSignal.class, s);
        }
    }

    @Test
    void handle_getRootPath_returnsHtml() throws IOException {
        handler.addSignal(new TrendingSignal("TestArticle", 7, 1000L, 2000L));

        HttpExchange exchange = mockExchange("GET", "/");
        ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(responseBody);

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        String body = responseBody.toString();
        assertTrue(body.contains("<!DOCTYPE html>"));
        assertTrue(body.contains("Wikipedia Intel"));
    }

    @Test
    void handle_getApiSignals_returnsJson() throws IOException {
        handler.addSignal(new TrendingSignal("TestArticle", 7, 1000L, 2000L));

        HttpExchange exchange = mockExchange("GET", "/api/signals");
        ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(responseBody);

        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(200), anyLong());
        String body = responseBody.toString();
        assertTrue(body.contains("TRENDING"));
        assertTrue(body.contains("TestArticle"));
    }

    private HttpExchange mockExchange(String method, String path) {
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestMethod()).thenReturn(method);
        when(exchange.getRequestURI()).thenReturn(URI.create(path));
        Headers headers = new Headers();
        when(exchange.getResponseHeaders()).thenReturn(headers);
        return exchange;
    }
}
