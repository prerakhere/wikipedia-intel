package com.wikipedia.intel.web;

import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DashboardServer — validates lifecycle and port binding.
 */
class DashboardServerTest {

    private DashboardServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void server_bindsToConfiguredPort_andAcceptsRequests() throws Exception {
        int port = findAvailablePort();
        SignalHandler handler = new SignalHandler(new SignalFormatter());
        handler.addSignal(new TrendingSignal("TestArticle", 5, 1000L, 2000L));

        server = new DashboardServer(port, handler);
        server.start();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/api/signals"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("TRENDING"));
        assertTrue(response.body().contains("TestArticle"));
    }

    @Test
    void startAndStop_lifecycle() throws Exception {
        int port = findAvailablePort();
        SignalHandler handler = new SignalHandler(new SignalFormatter());

        server = new DashboardServer(port, handler);
        server.start();

        // Server should respond while running
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        // Stop the server
        server.stop();
        server = null;

        // After stop, connection should be refused
        assertThrows(Exception.class, () -> {
            HttpClient newClient = HttpClient.newHttpClient();
            newClient.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/"))
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        });
    }

    @Test
    void portBindFailure_logsErrorAndTerminatesGracefully() {
        // Bind to a port first, then try to bind again
        int port = findAvailablePort();
        SignalHandler handler = new SignalHandler(new SignalFormatter());

        // First server grabs the port
        DashboardServer first = null;
        try {
            first = new DashboardServer(port, handler);
            first.start();

            // Second server should fail gracefully (IOException)
            assertThrows(IOException.class, () -> {
                new DashboardServer(port, handler);
            });
        } catch (IOException e) {
            fail("First server should bind successfully: " + e.getMessage());
        } finally {
            if (first != null) {
                first.stop();
            }
        }
    }

    private int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Cannot find available port", e);
        }
    }
}
