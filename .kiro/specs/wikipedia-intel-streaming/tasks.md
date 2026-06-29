# Implementation Plan: Wikipedia Intel Streaming

## Overview

Implement the Wikipedia real-time streaming pipeline in three independently runnable phases, following TDD methodology (tests first, then implementation). Each phase is a vertical slice that can be developed, tested, and demoed standalone.

## Tasks

- [x] 1. Shared Foundation: Data Models and Configuration
  - [x] 1.1 Add jqwik dependency to build.gradle for property-based testing
    - Add `testImplementation 'net.jqwik:jqwik:1.8.4'` to dependencies block
    - Add `testImplementation 'org.apache.kafka:kafka-streams-test-utils:3.7.0'` for TopologyTestDriver
    - _Requirements: 9.1_

  - [x] 1.2 Create PipelineConfig record with static load() method
    - Create `src/main/java/com/wikipedia/intel/config/PipelineConfig.java`
    - Implement system property > environment variable > default loading priority
    - Include all config fields: Kafka, SSE, trending, bot anomaly, DynamoDB, dashboard port
    - _Requirements: 5.2, 5.3, 6.1, 6.2, 4.1, 4.3, 8.1_

  - [x] 1.3 Write unit tests for WikipediaEvent model
    - Create `src/test/java/com/wikipedia/intel/model/WikipediaEventTest.java`
    - Test Jackson deserialization from real SSE JSON sample
    - Test `isMainNamespace()` returns true for namespace=0, false otherwise
    - Test `@JsonIgnoreProperties(ignoreUnknown = true)` handles extra fields
    - _Requirements: 1.2, 1.3, 2.1_

  - [x] 1.4 Create WikipediaEvent record with Jackson annotations
    - Create `src/main/java/com/wikipedia/intel/model/WikipediaEvent.java`
    - Implement as Java record with nested Revision and Length records
    - Add `isMainNamespace()` method
    - Make tests from 1.3 pass
    - _Requirements: 1.2, 1.3, 2.1_

  - [x] 1.5 Write unit tests for Signal types (TrendingSignal, BotAnomalySignal)
    - Create `src/test/java/com/wikipedia/intel/model/SignalTest.java`
    - Test JSON serialization/deserialization with `@JsonTypeInfo` polymorphism
    - Test signal type discriminator field in serialized JSON
    - _Requirements: 5.4, 6.3_

  - [x] 1.6 Create Signal sealed interface, TrendingSignal, and BotAnomalySignal records
    - Create `src/main/java/com/wikipedia/intel/model/Signal.java`
    - Create `src/main/java/com/wikipedia/intel/model/TrendingSignal.java`
    - Create `src/main/java/com/wikipedia/intel/model/BotAnomalySignal.java`
    - Make tests from 1.5 pass
    - _Requirements: 5.4, 6.3_

  - [ ]* 1.7 Write property test: WikipediaEvent serialization round-trip
    - **Property 1: WikipediaEvent Serialization Round-Trip**
    - Create `src/test/java/com/wikipedia/intel/model/WikipediaEventPropertyTest.java`
    - Use jqwik to generate random valid WikipediaEvent instances
    - Assert serialize → deserialize produces equal object
    - **Validates: Requirements 1.2, 1.3, 3.1**

  - [ ]* 1.8 Write property test: Namespace filter partition
    - **Property 2: Namespace Filter Partition**
    - Add to property test class or create `src/test/java/com/wikipedia/intel/model/NamespaceFilterPropertyTest.java`
    - Use jqwik to generate WikipediaEvents with random namespace values
    - Assert event passes filter iff namespace == 0
    - **Validates: Requirements 2.1, 2.2, 2.3**

- [x] 2. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 3. Phase 1: SSE Consumer and Kafka Producer
  - [x] 3.1 Write unit tests for SseReconnector backoff computation
    - Create `src/test/java/com/wikipedia/intel/sse/SseReconnectorTest.java`
    - Test initial delay is 1000ms at attempt 0
    - Test doubling: attempt 1 → 2000ms, attempt 2 → 4000ms, etc.
    - Test cap at 30000ms for high attempt numbers
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 3.2 Implement SseReconnector
    - Create `src/main/java/com/wikipedia/intel/sse/SseReconnector.java`
    - Implement `computeDelay(int attempt)` as `min(initialDelayMs * 2^attempt, maxDelayMs)`
    - Implement `initialDelay()` accessor
    - Make tests from 3.1 pass
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [ ]* 3.3 Write property test: Exponential backoff computation
    - **Property 4: Exponential Backoff Computation**
    - Create `src/test/java/com/wikipedia/intel/sse/SseReconnectorPropertyTest.java`
    - Use jqwik to generate random non-negative attempt numbers
    - Assert delay == min(1000 * 2^n, 30000) for all n
    - Assert delay never exceeds maxDelayMs
    - **Validates: Requirements 4.1, 4.2, 4.3**

  - [ ]* 3.4 Write property test: Backoff reset on success
    - **Property 5: Backoff Reset on Success**
    - Add to `SseReconnectorPropertyTest.java`
    - After any sequence of failures, verify attempt 0 always yields initialDelay
    - **Validates: Requirements 4.4**

  - [x] 3.5 Write unit tests for EventPublisher
    - Create `src/test/java/com/wikipedia/intel/sse/EventPublisherTest.java`
    - Test namespace=0 events are published (mock KafkaProducer)
    - Test namespace≠0 events are discarded
    - Test Kafka message key equals event title
    - Test failed send increments failedSendCount
    - _Requirements: 2.2, 2.3, 3.1, 3.2, 3.4_

  - [x] 3.6 Implement EventPublisher
    - Create `src/main/java/com/wikipedia/intel/sse/EventPublisher.java`
    - Inject KafkaProducer<String, String> and ObjectMapper
    - Implement `publish(WikipediaEvent)`: check namespace, serialize, produce with title as key
    - Track failedSendCount via callback
    - Make tests from 3.5 pass
    - _Requirements: 2.2, 2.3, 3.1, 3.2, 3.3, 3.4_

  - [ ]* 3.7 Write property test: Kafka message key equals event title
    - **Property 3: Kafka Message Key Equals Event Title**
    - Add to EventPublisher property tests or create dedicated file
    - Generate random WikipediaEvents with namespace=0
    - Assert ProducerRecord key == event.title() for all
    - **Validates: Requirements 3.2**

  - [x] 3.8 Write unit tests for SseClient
    - Create `src/test/java/com/wikipedia/intel/sse/SseClientTest.java`
    - Test SSE line parsing: extract `data:` payload from SSE format
    - Test malformed JSON lines are skipped with WARN log
    - Test connection callback is invoked with parsed WikipediaEvent
    - _Requirements: 1.1, 1.2, 10.1_

  - [x] 3.9 Implement SseClient
    - Create `src/main/java/com/wikipedia/intel/sse/SseClient.java`
    - Use `java.net.http.HttpClient` to connect to SSE endpoint
    - Parse SSE format (extract `data:` lines), deserialize JSON via Jackson
    - Invoke Consumer<WikipediaEvent> callback for each valid event
    - Log connection at INFO, parse failures at WARN
    - _Requirements: 1.1, 1.2, 1.3, 10.1_

  - [ ] 3.10 Write unit tests for SseIngestApp integration
    - Create `src/test/java/com/wikipedia/intel/sse/SseIngestAppTest.java`
    - Test app wires SseClient → EventPublisher correctly
    - Test reconnection loop invokes SseReconnector on disconnect
    - Test graceful shutdown on interrupt
    - _Requirements: 4.4, 4.5, 9.1, 10.4_

  - [ ] 3.11 Implement SseIngestApp entry point
    - Create `src/main/java/com/wikipedia/intel/sse/SseIngestApp.java`
    - Wire together: PipelineConfig, SseClient, SseReconnector, EventPublisher
    - Implement reconnection loop with backoff logging at INFO level
    - Add shutdown hook for graceful close
    - _Requirements: 1.1, 4.4, 4.5, 9.1, 10.1, 10.4_

  - [ ] 3.12 Add Gradle task for running Phase 1
    - Add `tasks.register('runPhase1', JavaExec)` with mainClass = SseIngestApp
    - Verify `./gradlew runPhase1` starts the SSE consumer standalone
    - _Requirements: 9.1_

- [ ] 4. Checkpoint - Ensure all Phase 1 tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Phase 2: Kafka Streams Signal Detection
  - [ ] 5.1 Write unit tests for TrendingDetector logic
    - Create `src/test/java/com/wikipedia/intel/streams/TrendingDetectorTest.java`
    - Use TopologyTestDriver to pipe events and verify signal emission
    - Test: 4 edits to same article in window → no signal
    - Test: 5+ edits to same article in window → TrendingSignal emitted
    - Test: signal contains correct title, editCount, window timestamps
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ] 5.2 Implement TrendingDetector
    - Create `src/main/java/com/wikipedia/intel/streams/TrendingDetector.java`
    - Implement windowed count aggregation grouped by title
    - Filter on configurable threshold, map to TrendingSignal
    - Return KStream for wiring into topology
    - Make tests from 5.1 pass
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ]* 5.3 Write property test: Trending signal detection correctness
    - **Property 6: Trending Signal Detection Correctness**
    - Create `src/test/java/com/wikipedia/intel/streams/TrendingDetectorPropertyTest.java`
    - Generate random event sequences for a single article
    - Assert signal emitted iff count > threshold, with correct fields
    - **Validates: Requirements 5.2, 5.3, 5.4**

  - [ ] 5.4 Write unit tests for BotAnomalyDetector logic
    - Create `src/test/java/com/wikipedia/intel/streams/BotAnomalyDetectorTest.java`
    - Use TopologyTestDriver to pipe events and verify signal emission
    - Test: 8 bot edits + 2 human edits (total 10, ratio 0.8) → no signal (not > 0.8)
    - Test: 9 bot edits + 1 human edit (ratio 0.9, total 10) → BotAnomalySignal emitted
    - Test: 5 bot edits + 0 human edits (total 5, ratio 1.0) → suppressed (volume < 10)
    - Test: signal contains correct botEditCount, totalEditCount, ratio, window timestamps
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ] 5.5 Implement BotAnomalyDetector
    - Create `src/main/java/com/wikipedia/intel/streams/BotAnomalyDetector.java`
    - Implement windowed aggregation with constant key, tracking total and bot counts
    - Filter on ratio threshold AND minimum volume, map to BotAnomalySignal
    - Return KStream for wiring into topology
    - Make tests from 5.4 pass
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ]* 5.6 Write property test: Bot anomaly signal detection correctness
    - **Property 7: Bot Anomaly Signal Detection Correctness**
    - Create `src/test/java/com/wikipedia/intel/streams/BotAnomalyDetectorPropertyTest.java`
    - Generate random event sets with varying bot/human mix
    - Assert signal emitted iff ratio > threshold AND total >= minimumVolume
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4**

  - [ ] 5.7 Write integration test for SignalTopology (full topology)
    - Create `src/test/java/com/wikipedia/intel/streams/SignalTopologyTest.java`
    - Use TopologyTestDriver with both branches wired together
    - Test end-to-end: events in → correct signals out on `wikipedia.signals`
    - Verify both trending and bot anomaly branches coexist without interference
    - _Requirements: 5.1, 6.1_

  - [ ] 5.8 Implement SignalTopology
    - Create `src/main/java/com/wikipedia/intel/streams/SignalTopology.java`
    - Wire TrendingDetector and BotAnomalyDetector into a single Kafka Streams Topology
    - Configure custom JSON Serdes for WikipediaEvent and Signal
    - Make tests from 5.7 pass
    - _Requirements: 5.1, 6.1_

  - [ ] 5.9 Implement SignalDetectorApp entry point
    - Create `src/main/java/com/wikipedia/intel/streams/SignalDetectorApp.java`
    - Wire PipelineConfig → SignalTopology → KafkaStreams
    - Add shutdown hook, log signal emissions at INFO level
    - _Requirements: 9.2, 10.3, 10.4_

  - [ ] 5.10 Add Gradle task for running Phase 2
    - Add `tasks.register('runPhase2', JavaExec)` with mainClass = SignalDetectorApp
    - Verify `./gradlew runPhase2` starts the signal detector standalone
    - _Requirements: 9.2_

- [ ] 6. Checkpoint - Ensure all Phase 2 tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Phase 3: DynamoDB Persistence and Web Dashboard
  - [ ] 7.1 Add AWS SDK DynamoDB dependency to build.gradle
    - Add `implementation 'software.amazon.awssdk:dynamodb:2.25.27'` to dependencies
    - Add `implementation 'software.amazon.awssdk:auth:2.25.27'` if not transitive
    - _Requirements: 7.1_

  - [ ] 7.2 Write unit tests for DynamoKeyStrategy
    - Create `src/test/java/com/wikipedia/intel/persist/DynamoKeyStrategyTest.java`
    - Test partition key = "TRENDING" for TrendingSignal
    - Test partition key = "BOT_ANOMALY" for BotAnomalySignal
    - Test sort key = windowEnd timestamp value
    - _Requirements: 7.2_

  - [ ] 7.3 Implement DynamoKeyStrategy
    - Create `src/main/java/com/wikipedia/intel/persist/DynamoKeyStrategy.java`
    - Implement `partitionKey(Signal)` and `sortKey(Signal)` methods
    - Make tests from 7.2 pass
    - _Requirements: 7.2_

  - [ ]* 7.4 Write property test: DynamoDB key derivation
    - **Property 8: DynamoDB Key Derivation**
    - Create `src/test/java/com/wikipedia/intel/persist/DynamoKeyStrategyPropertyTest.java`
    - Generate random Signal instances
    - Assert partitionKey == signalType() and sortKey == windowEnd() for all
    - **Validates: Requirements 7.2**

  - [ ] 7.5 Write unit tests for DynamoSignalWriter
    - Create `src/test/java/com/wikipedia/intel/persist/DynamoSignalWriterTest.java`
    - Mock DynamoDB client
    - Test successful write calls putItem with correct key/value
    - Test retry logic: first call fails, second succeeds → no error logged
    - Test 3 failures → logs ERROR
    - _Requirements: 7.1, 7.3_

  - [ ] 7.6 Implement DynamoSignalWriter
    - Create `src/main/java/com/wikipedia/intel/persist/DynamoSignalWriter.java`
    - Inject DynamoDbClient and DynamoKeyStrategy
    - Implement write with up to 3 retries and exponential backoff
    - Log failures at ERROR after retries exhausted
    - Make tests from 7.5 pass
    - _Requirements: 7.1, 7.2, 7.3_

  - [ ] 7.7 Write unit tests for SignalFormatter
    - Create `src/test/java/com/wikipedia/intel/web/SignalFormatterTest.java`
    - Test `formatHtmlRow(TrendingSignal)` includes: signal type, title, edit count, time window
    - Test `formatHtmlRow(BotAnomalySignal)` includes: signal type, bot ratio, counts, time window
    - Test `renderDashboardPage(signals)` produces valid HTML with table and auto-refresh script
    - Test `formatJson(signals)` produces valid JSON array with all signal fields
    - _Requirements: 8.2, 8.3, 8.4_

  - [ ] 7.8 Implement SignalFormatter
    - Create `src/main/java/com/wikipedia/intel/web/SignalFormatter.java`
    - Implement `formatHtmlRow(Signal)`: format as HTML `<tr>` with signal type, metrics, title (for trending), window timestamps
    - Implement `renderDashboardPage(List<Signal>)`: full HTML page with table and JavaScript polling `/api/signals` every 10 seconds
    - Implement `formatJson(List<Signal>)`: serialize signal list to JSON array using Jackson
    - Make tests from 7.7 pass
    - _Requirements: 8.2, 8.3, 8.4, 8.5_

  - [ ]* 7.9 Write property test: Dashboard display formatting completeness
    - **Property 9: Dashboard Display Formatting Completeness**
    - Create `src/test/java/com/wikipedia/intel/web/SignalFormatterPropertyTest.java`
    - Generate random Signal instances (both TrendingSignal and BotAnomalySignal)
    - Assert formatted HTML row contains signal type, metrics, and window boundaries
    - Assert TrendingSignal HTML contains article title
    - **Validates: Requirements 8.2, 8.3**

  - [ ] 7.10 Write unit tests for SignalHandler
    - Create `src/test/java/com/wikipedia/intel/web/SignalHandlerTest.java`
    - Test `addSignal()` stores signal in ring buffer
    - Test ring buffer caps at 50 signals (oldest evicted)
    - Test `recentSignals()` returns newest first
    - Test thread-safety: concurrent addSignal calls don't corrupt state
    - Test HTTP routing: GET `/` returns HTML, GET `/api/signals` returns JSON
    - _Requirements: 8.2, 8.4_

  - [ ] 7.11 Implement SignalHandler
    - Create `src/main/java/com/wikipedia/intel/web/SignalHandler.java`
    - Implement in-memory ring buffer (max 50 signals) using `ConcurrentLinkedDeque` or similar
    - Implement `addSignal(Signal)`: thread-safe insertion, evict oldest if over capacity
    - Implement `recentSignals()`: return up to 50 signals, newest first
    - Implement `handle(HttpExchange)`: route GET `/` → HTML page, GET `/api/signals` → JSON array
    - Make tests from 7.10 pass
    - _Requirements: 8.2, 8.4, 8.5_

  - [ ]* 7.12 Write property test: JSON API round-trip
    - **Property 10: JSON API Serialization Round-Trip**
    - Create `src/test/java/com/wikipedia/intel/web/SignalHandlerPropertyTest.java`
    - Generate random lists of Signal instances (up to 50)
    - Add to SignalHandler, call `formatJson(recentSignals())`, deserialize back
    - Assert deserialized list equals original signals (order preserved, newest first)
    - **Validates: Requirements 8.4**

  - [ ] 7.13 Write unit tests for DashboardServer
    - Create `src/test/java/com/wikipedia/intel/web/DashboardServerTest.java`
    - Test server binds to configured port and accepts requests
    - Test `start()` and `stop()` lifecycle
    - Test port bind failure logs ERROR and terminates gracefully
    - _Requirements: 8.1, 8.6_

  - [ ] 7.14 Implement DashboardServer
    - Create `src/main/java/com/wikipedia/intel/web/DashboardServer.java`
    - Use `com.sun.net.httpserver.HttpServer` to create embedded HTTP server
    - Configure server with SignalHandler for `/` and `/api/signals` contexts
    - Implement `start()`: bind and begin serving (non-blocking, uses internal executor)
    - Implement `stop()`: graceful shutdown with brief drain period
    - Handle port bind failure: log ERROR with port number, terminate gracefully
    - Make tests from 7.13 pass
    - _Requirements: 8.1, 8.6_

  - [ ] 7.15 Write integration test for Web Dashboard
    - Create `src/test/java/com/wikipedia/intel/web/DashboardIntegrationTest.java`
    - Start DashboardServer on a random available port
    - Add sample signals to SignalHandler
    - Hit `GET /api/signals` via HttpClient, verify JSON response structure
    - Hit `GET /`, verify HTML response contains signal data and auto-refresh script
    - Shut down server after test
    - _Requirements: 8.1, 8.2, 8.4, 8.5_

  - [ ] 7.16 Implement DashboardApp entry point
    - Create `src/main/java/com/wikipedia/intel/web/DashboardApp.java`
    - Consume from `wikipedia.signals` topic using KafkaConsumer
    - Deserialize signals, write to DynamoDB via DynamoSignalWriter
    - Add each signal to SignalHandler ring buffer
    - Start DashboardServer on configured port (default 8080)
    - Add shutdown hook for graceful close (stop server, close consumer)
    - Log startup at INFO with dashboard URL
    - _Requirements: 7.1, 8.1, 9.3, 10.4_

  - [ ] 7.17 Add Gradle task for running Phase 3
    - Add `tasks.register('runPhase3', JavaExec)` with mainClass = `com.wikipedia.intel.web.DashboardApp`
    - Verify `./gradlew runPhase3` starts the dashboard/persist consumer standalone
    - _Requirements: 9.3_

- [ ] 8. Final Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional property-based tests and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation between phases
- TDD workflow: test tasks come before implementation tasks (write test → implement → green)
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- Phase 2 integration tests use TopologyTestDriver (no running Kafka broker needed)
- Phase 3 web dashboard uses JDK built-in `com.sun.net.httpserver` — no external HTTP framework needed
- All phases are independently runnable via `./gradlew runPhase1`, `runPhase2`, `runPhase3`
- Dashboard auto-refreshes via JavaScript polling `/api/signals` every 10 seconds
- Ring buffer holds the 50 most recent signals in memory (newest first)

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "1.5"] },
    { "id": 2, "tasks": ["1.4", "1.6"] },
    { "id": 3, "tasks": ["1.7", "1.8", "3.1", "3.5"] },
    { "id": 4, "tasks": ["3.2", "3.6"] },
    { "id": 5, "tasks": ["3.3", "3.4", "3.7", "3.8"] },
    { "id": 6, "tasks": ["3.9"] },
    { "id": 7, "tasks": ["3.10"] },
    { "id": 8, "tasks": ["3.11", "3.12"] },
    { "id": 9, "tasks": ["5.1", "5.4"] },
    { "id": 10, "tasks": ["5.2", "5.5"] },
    { "id": 11, "tasks": ["5.3", "5.6", "5.7"] },
    { "id": 12, "tasks": ["5.8"] },
    { "id": 13, "tasks": ["5.9", "5.10"] },
    { "id": 14, "tasks": ["7.1"] },
    { "id": 15, "tasks": ["7.2", "7.7", "7.10"] },
    { "id": 16, "tasks": ["7.3", "7.8", "7.11"] },
    { "id": 17, "tasks": ["7.4", "7.5", "7.9", "7.12", "7.13"] },
    { "id": 18, "tasks": ["7.6", "7.14"] },
    { "id": 19, "tasks": ["7.15", "7.16"] },
    { "id": 20, "tasks": ["7.17"] }
  ]
}
```
