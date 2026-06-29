package com.wikipedia.intel.sse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.WikipediaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Connects to Wikipedia EventStreams SSE endpoint and delivers parsed events.
 * Uses java.net.http.HttpClient for the HTTP connection.
 */
public class SseClient {

    private static final Logger log = LoggerFactory.getLogger(SseClient.class);

    private final String url;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private volatile boolean running;

    public SseClient(String url, ObjectMapper mapper) {
        this.url = url;
        this.mapper = mapper;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.running = true;
    }

    /**
     * Opens an SSE connection and delivers parsed events to the callback.
     * Blocks the calling thread until the connection drops or is closed.
     *
     * @param onEvent callback invoked for each successfully parsed WikipediaEvent
     */
    public void connect(Consumer<WikipediaEvent> onEvent) {
        log.info("Connecting to SSE endpoint: {}", url);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "text/event-stream")
                .header("User-Agent", "wikipedia-intel/0.1.0 (https://github.com/prerakhere/wikipedia-intel)")
                .GET()
                .build();

        try {
            HttpResponse<InputStream> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() != 200) {
                log.warn("SSE endpoint returned HTTP {}", response.statusCode());
                return;
            }

            processStream(response.body(), onEvent);
        } catch (IOException | InterruptedException e) {
            if (running) {
                log.warn("SSE connection interrupted: {}", e.getMessage());
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Processes an SSE stream, extracting data lines and deserializing events.
     * Package-private for testability.
     */
    void processStream(InputStream stream, Consumer<WikipediaEvent> onEvent) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!running) {
                    break;
                }

                String jsonPayload = extractDataPayload(line);
                if (jsonPayload == null) {
                    continue;
                }

                try {
                    WikipediaEvent event = mapper.readValue(jsonPayload, WikipediaEvent.class);
                    onEvent.accept(event);
                } catch (Exception e) {
                    log.warn("Failed to parse SSE event JSON: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            if (running) {
                log.warn("Error reading SSE stream: {}", e.getMessage());
            }
        }
    }

    /**
     * Extracts the JSON payload from a line starting with "data:" or "data: ".
     * Returns null if the line is not a data line.
     */
    private String extractDataPayload(String line) {
        if (line.startsWith("data: ")) {
            return line.substring(6);
        } else if (line.startsWith("data:")) {
            return line.substring(5);
        }
        return null;
    }

    /** Gracefully closes the SSE connection. */
    public void close() {
        running = false;
    }
}
