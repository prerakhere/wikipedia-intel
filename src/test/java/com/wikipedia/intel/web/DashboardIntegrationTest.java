package com.wikipedia.intel.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the Web Dashboard — starts a real DashboardServer
 * on a random port, adds sample signals, and verifies HTTP responses.
 */
class DashboardIntegrationTest {

    private DashboardServer server;
    private SignalHandler handler;
    private int port;
    private HttpClient client;

    @BeforeEach
    void setUp() throws IOException {
        port = findAvailablePort();
        SignalFormatter formatter = new SignalFormatter();
        handler = new SignalHandler(formatter);

        long now = System.currentTimeMillis();
        // Add sample signals with recent timestamps
        handler.addSignal(new TrendingSignal("Java_(programming_language)", 12, List.of(), now - 300000, now));
        handler.addSignal(new BotAnomalySignal(18, 20, 0.9, now - 300000, now));
        handler.addSignal(new TrendingSignal("Kafka_(software)", 7, List.of(), now - 300000, now));

        server = new DashboardServer(port, handler);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void getApiSignals_returnsJsonWithCorrectStructure() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/api/signals"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("")
                .contains("application/json"));

        // Deserialize and verify structure
        ObjectMapper mapper = new ObjectMapper();
        List<Signal> signals = mapper.readValue(response.body(), new TypeReference<>() {});

        assertEquals(3, signals.size());

        // Sorted by edit count descending — BotAnomaly(20) > Java(12) > Kafka(7)
        Signal first = signals.get(0);
        assertInstanceOf(BotAnomalySignal.class, first);
        BotAnomalySignal botAnomaly = (BotAnomalySignal) first;
        assertEquals(18, botAnomaly.botEditCount());
        assertEquals(20, botAnomaly.totalEditCount());
        assertEquals(0.9, botAnomaly.ratio(), 0.001);

        Signal second = signals.get(1);
        assertInstanceOf(TrendingSignal.class, second);
        assertEquals("Java_(programming_language)", ((TrendingSignal) second).title());
        assertEquals(12, ((TrendingSignal) second).editCount());

        Signal third = signals.get(2);
        assertInstanceOf(TrendingSignal.class, third);
        assertEquals("Kafka_(software)", ((TrendingSignal) third).title());
        assertEquals(7, ((TrendingSignal) third).editCount());
    }

    @Test
    void getRoot_returnsHtmlWithSignalDataAndAutoRefreshScript() throws Exception {
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("")
                .contains("text/html"));

        String body = response.body();

        // Verify HTML contains signal data
        assertTrue(body.contains("TRENDING"), "HTML should contain TRENDING signal type");
        assertTrue(body.contains("BOT_ANOMALY"), "HTML should contain BOT_ANOMALY signal type");
        assertTrue(body.contains("Java_(programming_language)"), "HTML should contain trending article title");
        assertTrue(body.contains("Kafka_(software)"), "HTML should contain trending article title");

        // Verify auto-refresh script is present
        assertTrue(body.contains("<script>"), "HTML should contain script tag");
        assertTrue(body.contains("setInterval"), "HTML should have auto-refresh via setInterval");
        assertTrue(body.contains("/api/signals"), "Script should poll /api/signals endpoint");
        assertTrue(body.contains("10000"), "Script should refresh every 10 seconds (10000ms)");
    }

    @Test
    void getApiSignals_afterAddingMoreSignals_reflectsUpdates() throws Exception {
        // Add another signal after server is running
        long now = System.currentTimeMillis();
        handler.addSignal(new TrendingSignal("New_Article", 15, List.of(), now - 300000, now));

        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/api/signals"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());

        ObjectMapper mapper = new ObjectMapper();
        List<Signal> signals = mapper.readValue(response.body(), new TypeReference<>() {});

        assertEquals(4, signals.size());
        // Sorted by edit count: BotAnomaly(20) > New_Article(15) > Java(12) > Kafka(7)
        assertInstanceOf(BotAnomalySignal.class, signals.get(0));
        assertInstanceOf(TrendingSignal.class, signals.get(1));
        assertEquals("New_Article", ((TrendingSignal) signals.get(1)).title());
        assertEquals(15, ((TrendingSignal) signals.get(1)).editCount());
    }

    private int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Cannot find available port", e);
        }
    }
}
