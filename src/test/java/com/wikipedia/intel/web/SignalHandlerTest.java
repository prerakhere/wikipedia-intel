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
        long now = System.currentTimeMillis();
        TrendingSignal signal = new TrendingSignal("Java", 10, List.of(), now - 300000, now);
        handler.addSignal(signal);

        List<Signal> signals = handler.recentSignals();
        assertEquals(1, signals.size());
        assertEquals(signal, signals.get(0));
    }

    @Test
    void ringBuffer_capsAt50Signals_oldestEvicted() {
        long now = System.currentTimeMillis();
        // Add 55 signals — oldest 5 should be evicted
        for (int i = 0; i < 55; i++) {
            handler.addSignal(new TrendingSignal("Article-" + i, i + 1, List.of(), now - 300000, now));
        }

        List<Signal> signals = handler.recentSignals();
        assertEquals(50, signals.size());
    }

    @Test
    void recentSignals_returnsSortedByEditCountDescending() {
        long now = System.currentTimeMillis();
        handler.addSignal(new TrendingSignal("Low", 3, List.of(), now - 300000, now));
        handler.addSignal(new TrendingSignal("High", 15, List.of(), now - 300000, now));
        handler.addSignal(new TrendingSignal("Medium", 8, List.of(), now - 300000, now));

        List<Signal> signals = handler.recentSignals();
        assertEquals(3, signals.size());
        assertEquals("High", ((TrendingSignal) signals.get(0)).title());
        assertEquals("Medium", ((TrendingSignal) signals.get(1)).title());
        assertEquals("Low", ((TrendingSignal) signals.get(2)).title());
    }

    @Test
    void recentSignals_botAnomalySortedByTotalEditCount() {
        long now = System.currentTimeMillis();
        handler.addSignal(new BotAnomalySignal(5, 6, 0.83, now - 300000, now));
        handler.addSignal(new TrendingSignal("Big", 20, List.of(), now - 300000, now));
        handler.addSignal(new BotAnomalySignal(8, 10, 0.8, now - 300000, now));

        List<Signal> signals = handler.recentSignals();
        assertEquals(3, signals.size());
        // 20 edits > 10 total > 6 total
        assertEquals("Big", ((TrendingSignal) signals.get(0)).title());
        assertEquals(10, ((BotAnomalySignal) signals.get(1)).totalEditCount());
        assertEquals(6, ((BotAnomalySignal) signals.get(2)).totalEditCount());
    }

    @Test
    void recentSignals_excludesSignalsOlderThan10Minutes() {
        long now = System.currentTimeMillis();
        long elevenMinutesAgo = now - (11 * 60 * 1000);
        long fiveMinutesAgo = now - (5 * 60 * 1000);

        // Old signal — windowEnd is 11 minutes ago
        handler.addSignal(new TrendingSignal("Old_Article", 20, List.of(), elevenMinutesAgo - 300000, elevenMinutesAgo));
        // Recent signal — windowEnd is 5 minutes ago
        handler.addSignal(new TrendingSignal("Recent_Article", 8, List.of(), fiveMinutesAgo - 300000, fiveMinutesAgo));

        List<Signal> signals = handler.recentSignals();
        assertEquals(1, signals.size());
        assertEquals("Recent_Article", ((TrendingSignal) signals.get(0)).title());
    }

    @Test
    void recentSignals_includesSignalsExactly10MinutesOld() {
        long now = System.currentTimeMillis();
        long tenMinutesAgo = now - (10 * 60 * 1000);

        handler.addSignal(new TrendingSignal("Boundary_Article", 5, List.of(), tenMinutesAgo - 300000, tenMinutesAgo));

        List<Signal> signals = handler.recentSignals();
        assertEquals(1, signals.size());
    }

    @Test
    void threadSafety_concurrentAddSignalCallsDontCorruptState() throws InterruptedException {
        int threadCount = 10;
        int signalsPerThread = 20;
        long now = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < signalsPerThread; i++) {
                        handler.addSignal(new TrendingSignal(
                                "T" + threadId + "-" + i, i + 1, List.of(), now - 300000, now));
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
        long now = System.currentTimeMillis();
        handler.addSignal(new TrendingSignal("TestArticle", 7, List.of(), now - 300000, now));

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
        long now = System.currentTimeMillis();
        handler.addSignal(new TrendingSignal("TestArticle", 7, List.of(), now - 300000, now));

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
