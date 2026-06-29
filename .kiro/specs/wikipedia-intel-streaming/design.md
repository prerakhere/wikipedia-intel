# Design Document

## Overview

A real-time streaming pipeline that ingests Wikipedia's live edit stream, produces filtered events to Kafka, detects signals (trending articles, bot anomalies) via Kafka Streams windowed aggregations, persists results to DynamoDB, and displays them on a lightweight web dashboard. Built as three independently runnable phases in plain Java 21.

## Architecture

### High-Level Data Flow

```
Wikipedia SSE ──► SSE_Consumer ──► Namespace Filter ──► Kafka_Producer
                                                              │
                                                              ▼
                                               wikipedia.recentchanges topic
                                                              │
                                                              ▼
                                                      Signal_Detector
                                                    (Kafka Streams app)
                                                              │
                                                              ▼
                                                  wikipedia.signals topic
                                                        │           │
                                                        ▼           ▼
                                              Persistence_Layer   Web_Dashboard
                                                (DynamoDB)       (HTTP server)
```

### Phase Boundaries

| Phase | Components | Entry Point | Standalone? |
|-------|-----------|-------------|-------------|
| Phase 1 | SSE_Consumer, Kafka_Producer | `SseIngestApp.main()` | Yes |
| Phase 2 | Signal_Detector (Kafka Streams) | `SignalDetectorApp.main()` | Yes |
| Phase 3 | Persistence_Layer, Web_Dashboard | `DashboardApp.main()` | Yes |

Each phase runs as its own JVM process. Phase 2 only needs `wikipedia.recentchanges` to contain data (not Phase 1 running live). Phase 3 only needs `wikipedia.signals` to contain data.

## Package Structure

```
com.wikipedia.intel
├── App.java                          # Original entry point (kept for backward compat)
├── KafkaVerify.java                  # Existing smoke test
│
├── sse/                              # Phase 1: SSE ingestion
│   ├── SseIngestApp.java             # Phase 1 entry point
│   ├── SseClient.java               # HTTP/SSE connection + event stream
│   ├── SseReconnector.java           # Exponential backoff reconnection logic
│   └── EventPublisher.java           # Filters + produces to Kafka
│
├── model/                            # Shared data models
│   ├── WikipediaEvent.java           # Deserialized SSE event (record)
│   ├── TrendingSignal.java           # Trending article signal (record)
│   ├── BotAnomalySignal.java         # Bot anomaly signal (record)
│   └── Signal.java                   # Sealed interface for signal types
│
├── streams/                          # Phase 2: Signal detection
│   ├── SignalDetectorApp.java        # Phase 2 entry point
│   ├── SignalTopology.java           # Kafka Streams topology builder
│   ├── TrendingDetector.java         # Windowed edit-count aggregation
│   └── BotAnomalyDetector.java       # Windowed bot-ratio aggregation
│
├── persist/                          # Phase 3: DynamoDB persistence
│   ├── DynamoSignalWriter.java       # DynamoDB write with retry
│   └── DynamoKeyStrategy.java        # Key generation logic
│
├── web/                              # Phase 3: Web dashboard
│   ├── DashboardApp.java             # Phase 3 entry point (starts consumer + server)
│   ├── DashboardServer.java          # HTTP server using com.sun.net.httpserver
│   ├── SignalHandler.java            # Request handlers for / and /api/signals
│   └── SignalFormatter.java          # Signal → HTML table row / JSON formatting
│
└── config/                           # Configuration
    └── PipelineConfig.java           # Centralized configuration loading
```

## Data Models

### WikipediaEvent

```java
package com.wikipedia.intel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Deserialized Wikipedia EventStreams SSE event.
 * Immutable record mapping the JSON fields we care about.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikipediaEvent(
    String title,
    String wiki,
    String user,
    boolean bot,
    long timestamp,
    String type,
    String comment,
    @JsonProperty("namespace") int namespace,
    Revision revision,
    Length length
) {
    public record Revision(long old, @JsonProperty("new") long current) {}
    public record Length(int old, @JsonProperty("new") int current) {}

    /** Returns true if this event belongs to the main article namespace. */
    public boolean isMainNamespace() {
        return namespace == 0;
    }
}
```

### Signal (Sealed Interface)

```java
package com.wikipedia.intel.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed interface for all signal types emitted by the Signal_Detector.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "signalType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = TrendingSignal.class, name = "TRENDING"),
    @JsonSubTypes.Type(value = BotAnomalySignal.class, name = "BOT_ANOMALY")
})
public sealed interface Signal permits TrendingSignal, BotAnomalySignal {
    String signalType();
    long windowStart();
    long windowEnd();
}
```

### TrendingSignal

```java
package com.wikipedia.intel.model;

public record TrendingSignal(
    String title,
    int editCount,
    long windowStart,
    long windowEnd
) implements Signal {
    @Override
    public String signalType() { return "TRENDING"; }
}
```

### BotAnomalySignal

```java
package com.wikipedia.intel.model;

public record BotAnomalySignal(
    int botEditCount,
    int totalEditCount,
    double ratio,
    long windowStart,
    long windowEnd
) implements Signal {
    @Override
    public String signalType() { return "BOT_ANOMALY"; }
}
```

## Components and Interfaces

### SseClient

```java
package com.wikipedia.intel.sse;

import com.wikipedia.intel.model.WikipediaEvent;
import java.util.function.Consumer;

/**
 * Connects to Wikipedia EventStreams SSE endpoint and delivers parsed events.
 * Uses java.net.http.HttpClient for the HTTP connection.
 */
public class SseClient {

    /**
     * Opens an SSE connection and delivers parsed events to the callback.
     * Blocks the calling thread until the connection drops or is closed.
     *
     * @param onEvent callback invoked for each successfully parsed WikipediaEvent
     * @throws SseConnectionException if the initial connection cannot be established
     */
    public void connect(Consumer<WikipediaEvent> onEvent) { /* ... */ }

    /** Gracefully closes the SSE connection. */
    public void close() { /* ... */ }
}
```

### SseReconnector

```java
package com.wikipedia.intel.sse;

/**
 * Manages exponential backoff for SSE reconnection.
 * Thread-safe, stateless computation — backoff state is passed in/out.
 */
public class SseReconnector {

    private final long initialDelayMs;
    private final long maxDelayMs;

    public SseReconnector(long initialDelayMs, long maxDelayMs) { /* ... */ }

    /**
     * Computes the next backoff delay given the current attempt number.
     * @param attempt zero-based attempt number (0 = first failure)
     * @return delay in milliseconds, capped at maxDelayMs
     */
    public long computeDelay(int attempt) {
        return Math.min(initialDelayMs * (1L << attempt), maxDelayMs);
    }

    /** Returns the initial delay (used after successful reconnection to reset state). */
    public long initialDelay() { return initialDelayMs; }
}
```

### EventPublisher

```java
package com.wikipedia.intel.sse;

import com.wikipedia.intel.model.WikipediaEvent;

/**
 * Receives WikipediaEvents, applies namespace filter, serializes to JSON,
 * and produces to the wikipedia.recentchanges Kafka topic.
 */
public class EventPublisher {

    /**
     * Filters and publishes a single event.
     * @return true if the event was published (namespace == 0), false if discarded
     */
    public boolean publish(WikipediaEvent event) { /* ... */ }

    /** Returns the count of failed sends since startup. */
    public long failedSendCount() { /* ... */ }
}
```

### SignalTopology

```java
package com.wikipedia.intel.streams;

import org.apache.kafka.streams.Topology;

/**
 * Builds the Kafka Streams topology for signal detection.
 * Encapsulates both trending and bot-anomaly detection in a single topology.
 */
public class SignalTopology {

    /**
     * Builds and returns the complete detection topology.
     */
    public Topology build() { /* ... */ }
}
```

### DynamoKeyStrategy

```java
package com.wikipedia.intel.persist;

import com.wikipedia.intel.model.Signal;

/**
 * Derives DynamoDB partition key and sort key from a Signal.
 */
public class DynamoKeyStrategy {

    /** Partition key = signal type (e.g., "TRENDING", "BOT_ANOMALY"). */
    public String partitionKey(Signal signal) {
        return signal.signalType();
    }

    /** Sort key = window end timestamp as ISO-8601 string for lexicographic ordering. */
    public String sortKey(Signal signal) {
        return String.valueOf(signal.windowEnd());
    }
}
```

### DashboardServer

```java
package com.wikipedia.intel.web;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;

/**
 * Lightweight embedded HTTP server using JDK's built-in com.sun.net.httpserver.
 * Serves the signal dashboard HTML page and JSON API.
 */
public class DashboardServer {

    private final HttpServer server;
    private final SignalHandler handler;

    /**
     * Creates and configures the HTTP server on the given port.
     *
     * @param port TCP port to bind (default: 8080)
     * @param handler the request handler for serving dashboard content
     * @throws java.io.IOException if the port cannot be bound
     */
    public DashboardServer(int port, SignalHandler handler) { /* ... */ }

    /** Starts the HTTP server. Non-blocking — uses an internal executor. */
    public void start() { /* ... */ }

    /** Stops the HTTP server gracefully with a brief drain period. */
    public void stop() { /* ... */ }
}
```

### SignalHandler

```java
package com.wikipedia.intel.web;

import com.sun.net.httpserver.HttpHandler;
import com.wikipedia.intel.model.Signal;
import java.util.List;

/**
 * Handles HTTP requests for the dashboard.
 * Routes:
 *   GET /           → HTML page with signal table (auto-refreshing)
 *   GET /api/signals → JSON array of recent signals
 */
public class SignalHandler implements HttpHandler {

    private static final int MAX_SIGNALS = 50;

    /**
     * Adds a signal to the in-memory ring buffer.
     * Thread-safe — called from the Kafka consumer thread.
     */
    public void addSignal(Signal signal) { /* ... */ }

    /**
     * Returns the most recent signals (up to MAX_SIGNALS), newest first.
     */
    public List<Signal> recentSignals() { /* ... */ }

    /** Handles incoming HTTP requests, dispatching by path. */
    @Override
    public void handle(com.sun.net.httpserver.HttpExchange exchange) { /* ... */ }
}
```

### SignalFormatter

```java
package com.wikipedia.intel.web;

import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import com.wikipedia.intel.model.BotAnomalySignal;
import java.util.List;

/**
 * Formats Signal instances for dashboard display (HTML and JSON).
 */
public class SignalFormatter {

    /**
     * Formats a signal as an HTML table row.
     * Includes: signal type, metrics, article title (if trending), time window.
     */
    public String formatHtmlRow(Signal signal) { /* ... */ }

    /**
     * Renders the full HTML dashboard page with the given signals embedded in a table.
     * Includes auto-refresh JavaScript that polls /api/signals every 10 seconds.
     */
    public String renderDashboardPage(List<Signal> signals) { /* ... */ }

    /**
     * Serializes a list of signals to a JSON array string using Jackson.
     */
    public String formatJson(List<Signal> signals) { /* ... */ }
}
```

## Kafka Streams Topology (Phase 2)

```
                    wikipedia.recentchanges
                              │
                              ▼
                        Source Node
                              │
                    ┌─────────┴─────────┐
                    ▼                     ▼
          [Branch: by title]     [Branch: global counts]
                    │                     │
                    ▼                     ▼
          GroupBy(title)          GroupBy(constant key)
                    │                     │
                    ▼                     ▼
    WindowedBy(tumbling 5min)   WindowedBy(tumbling 5min)
                    │                     │
                    ▼                     ▼
          Count per article       Aggregate(total, botCount)
                    │                     │
                    ▼                     ▼
       Filter(count > threshold)  Filter(ratio > threshold
                    │                        AND total >= 10)
                    ▼                     ▼
         Map → TrendingSignal    Map → BotAnomalySignal
                    │                     │
                    └─────────┬───────────┘
                              ▼
                    wikipedia.signals (Sink)
```

### Topology Details

- **Input topic:** `wikipedia.recentchanges` with String key (article title), JSON value
- **Trending branch:** Groups by title (the existing key), uses tumbling window (default 5 min), counts events, filters on threshold (default 5), maps to `TrendingSignal` JSON
- **Bot anomaly branch:** Groups by a constant key (all events in one group), uses tumbling window (default 5 min), aggregates into `{total: int, botCount: int}`, computes ratio, filters on threshold (default 0.8) AND minimum volume (default 10), maps to `BotAnomalySignal` JSON
- **Output topic:** `wikipedia.signals` with String key (signal type), JSON value
- **Serdes:** Custom JSON Serde using Jackson for `WikipediaEvent` and `Signal`

## Configuration

Configuration is loaded from system properties and environment variables, with sensible defaults for local development.

```java
package com.wikipedia.intel.config;

/**
 * Centralized configuration for all pipeline components.
 * Loads from system properties > environment variables > defaults.
 */
public record PipelineConfig(
    // Kafka
    String bootstrapServers,          // default: localhost:9092
    String inputTopic,                // default: wikipedia.recentchanges
    String signalsTopic,              // default: wikipedia.signals

    // SSE
    String sseUrl,                    // default: https://stream.wikimedia.org/v2/stream/recentchange
    long reconnectInitialMs,          // default: 1000
    long reconnectMaxMs,              // default: 30000

    // Trending detection
    long trendingWindowMinutes,       // default: 5
    int trendingThreshold,            // default: 5

    // Bot anomaly detection
    long botWindowMinutes,            // default: 5
    double botRatioThreshold,         // default: 0.8
    int botMinimumVolume,             // default: 10

    // DynamoDB
    String dynamoTableName,           // default: wikipedia-signals
    String dynamoRegion,              // default: ap-south-1
    int dynamoMaxRetries,             // default: 3

    // Web Dashboard
    int dashboardPort                 // default: 8080
) {
    /** Loads config from system properties and environment, applying defaults. */
    public static PipelineConfig load() { /* ... */ }
}
```

**Loading Priority:** System property (`-Dpipeline.bootstrap.servers=...`) > Environment variable (`PIPELINE_BOOTSTRAP_SERVERS`) > Default value.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| SSE connection drop | Log WARN, enter exponential backoff (1s → 30s max) |
| SSE parse failure (bad JSON) | Log WARN, skip event, continue stream |
| Kafka send failure | Log ERROR, increment `failedSendCount`, continue |
| Kafka Streams uncaught exception | Log ERROR, terminate gracefully |
| DynamoDB write failure | Retry 3× with backoff, then log ERROR |
| Dashboard port bind failure | Log ERROR with port number, terminate gracefully |
| Unrecoverable error (any component) | Log ERROR with exception, shut down cleanly |

## Testing Strategy

### Unit Tests (JUnit 5)
- **Model serialization:** Round-trip JSON tests for `WikipediaEvent`, `TrendingSignal`, `BotAnomalySignal`
- **Namespace filter:** Verify `isMainNamespace()` logic
- **Backoff computation:** Verify delay calculation at various attempt numbers
- **Key strategy:** Verify DynamoDB key derivation
- **Signal formatting:** Verify `SignalFormatter` produces correct HTML rows and JSON output containing required fields
- **Signal handler:** Verify ring buffer behavior (max 50 signals, newest first)

### Property-Based Tests (JUnit 5 + jqwik or custom generators)
- Serialization round-trips across random valid events
- Namespace filter partition across random namespace values
- Backoff invariant (never exceeds max) across random attempt counts
- Signal detection correctness across random event sequences
- Dashboard formatting completeness across random signals
- JSON API round-trip across random signal lists

### Integration Tests (TopologyTestDriver)
- **Kafka Streams:** Use `TopologyTestDriver` to test the full topology without a running broker
- **Trending detection:** Pipe events through, verify signal emission at threshold
- **Bot anomaly detection:** Pipe mixed bot/human events, verify ratio logic and minimum volume guard
- **Web Dashboard:** Start embedded `DashboardServer`, hit `/api/signals`, verify JSON response structure and content

### Smoke Tests
- Phase 1 standalone startup (mocked SSE endpoint)
- Phase 2 standalone startup (pre-populated test topic via TopologyTestDriver)
- Phase 3 standalone startup (mocked DynamoDB + in-memory signals)

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: WikipediaEvent Serialization Round-Trip

*For any* valid `WikipediaEvent` instance, serializing it to JSON with Jackson and then deserializing the JSON back to a `WikipediaEvent` SHALL produce an object equal to the original.

**Validates: Requirements 1.2, 1.3, 3.1**

### Property 2: Namespace Filter Partition

*For any* `WikipediaEvent`, the event passes the namespace filter (is published to Kafka) if and only if its `namespace` field equals 0.

**Validates: Requirements 2.1, 2.2, 2.3**

### Property 3: Kafka Message Key Equals Event Title

*For any* `WikipediaEvent` that passes the namespace filter, the Kafka `ProducerRecord` key SHALL equal the event's `title` field.

**Validates: Requirements 3.2**

### Property 4: Exponential Backoff Computation

*For any* non-negative attempt number `n`, the computed backoff delay SHALL equal `min(initialDelay * 2^n, maxDelay)` milliseconds — specifically `min(1000 * 2^n, 30000)` with default configuration.

**Validates: Requirements 4.1, 4.2, 4.3**

### Property 5: Backoff Reset on Success

*For any* backoff state (regardless of how many failures preceded), after a successful reconnection the next failure SHALL produce a delay of exactly 1000 milliseconds (attempt 0).

**Validates: Requirements 4.4**

### Property 6: Trending Signal Detection Correctness

*For any* sequence of `WikipediaEvent` instances for a single article within a tumbling window, the `Signal_Detector` SHALL emit a `TrendingSignal` if and only if the event count exceeds the configured threshold, and the signal SHALL contain the correct article title, edit count, window start, and window end.

**Validates: Requirements 5.2, 5.3, 5.4**

### Property 7: Bot Anomaly Signal Detection Correctness

*For any* set of `WikipediaEvent` instances within a tumbling window, the `Signal_Detector` SHALL emit a `BotAnomalySignal` if and only if `botCount / totalCount > threshold` AND `totalCount >= minimumVolume`, and the signal SHALL contain the correct bot count, total count, ratio, window start, and window end.

**Validates: Requirements 6.1, 6.2, 6.3, 6.4**

### Property 8: DynamoDB Key Derivation

*For any* `Signal` instance, the generated partition key SHALL equal the signal's `signalType()` and the generated sort key SHALL equal the signal's `windowEnd()` value.

**Validates: Requirements 7.2**

### Property 9: Dashboard Display Formatting Completeness

*For any* `Signal` instance, the formatted dashboard output (HTML row) SHALL contain the signal type, the relevant metrics (edit count for Trending, bot ratio for Bot Anomaly), the article title (for Trending signals), and the time window boundaries (start and end timestamps).

**Validates: Requirements 8.2, 8.3**

### Property 10: JSON API Serialization Round-Trip

*For any* list of `Signal` instances stored in the dashboard, serializing via the `/api/signals` JSON endpoint and deserializing the response SHALL produce a list equivalent to the original signals (up to the most recent 50).

**Validates: Requirements 8.4**
