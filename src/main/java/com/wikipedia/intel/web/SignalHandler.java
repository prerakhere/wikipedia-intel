package com.wikipedia.intel.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.wikipedia.intel.model.Signal;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Handles HTTP requests for the dashboard and maintains an in-memory
 * ring buffer of the most recent signals.
 *
 * <p>Routes:
 * <ul>
 *   <li>GET / → HTML page with signal table (auto-refreshing)</li>
 *   <li>GET /api/signals → JSON array of recent signals</li>
 * </ul>
 */
public class SignalHandler implements HttpHandler {

    private static final int MAX_SIGNALS = 50;

    private final ConcurrentLinkedDeque<Signal> buffer = new ConcurrentLinkedDeque<>();
    private final SignalFormatter formatter;

    public SignalHandler(SignalFormatter formatter) {
        this.formatter = formatter;
    }

    /**
     * Adds a signal to the in-memory ring buffer.
     * Thread-safe — called from the Kafka consumer thread.
     * Newest signals are at the head; oldest evicted when over capacity.
     */
    public void addSignal(Signal signal) {
        buffer.addFirst(signal);
        // Trim to capacity — remove from tail (oldest)
        while (buffer.size() > MAX_SIGNALS) {
            buffer.pollLast();
        }
    }

    /**
     * Returns the most recent signals (up to MAX_SIGNALS), newest first.
     * Returns a snapshot — safe to iterate without external synchronization.
     */
    public List<Signal> recentSignals() {
        return new ArrayList<>(buffer);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String response;
        String contentType;

        if ("/api/signals".equals(path)) {
            contentType = "application/json; charset=UTF-8";
            response = formatter.formatJson(recentSignals());
        } else {
            contentType = "text/html; charset=UTF-8";
            response = formatter.renderDashboardPage(recentSignals());
        }

        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
