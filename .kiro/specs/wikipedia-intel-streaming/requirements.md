# Requirements Document

## Introduction

A real-time streaming analytics system that consumes Wikipedia's live edit stream via SSE, produces events to Kafka, detects meaningful signals (trending articles, bot anomalies) using Kafka Streams, persists results to DynamoDB, and displays results via a web dashboard. The system is built in vertical phases, each independently runnable and testable.

## Glossary

- **SSE_Consumer**: The component that connects to Wikipedia EventStreams via Server-Sent Events and receives raw edit events
- **Kafka_Producer**: The component that serializes filtered Wikipedia events and publishes them to the `wikipedia.recentchanges` Kafka topic
- **Signal_Detector**: The Kafka Streams application that reads from `wikipedia.recentchanges`, applies windowed aggregations, and emits detected patterns to the `wikipedia.signals` topic
- **Persistence_Layer**: The component that reads from `wikipedia.signals` and stores detected signals in DynamoDB
- **Web_Dashboard**: A lightweight embedded HTTP server that serves an HTML page displaying recent detected signals, using the JDK built-in `com.sun.net.httpserver` or a minimal library such as Javalin
- **Wikipedia_Event**: A JSON object from Wikipedia EventStreams containing fields: title, wiki, user, bot, timestamp, type, comment, length (old/new), namespace, revision
- **Namespace_0**: The main article namespace in Wikipedia (excludes talk pages, user pages, categories, and other non-article namespaces)
- **Trending_Signal**: A signal emitted when a single article receives edits exceeding the configured frequency threshold within a time window
- **Bot_Anomaly_Signal**: A signal emitted when the ratio of bot edits to total edits within a time window exceeds the configured threshold
- **Reconnection_Backoff**: An exponential backoff strategy that increases wait time between SSE reconnection attempts up to a configured maximum delay

## Requirements

### Requirement 1: SSE Connection and Event Ingestion

**User Story:** As a system operator, I want the system to connect to Wikipedia EventStreams and continuously receive edit events, so that the pipeline has a reliable source of real-time data.

#### Acceptance Criteria

1. WHEN the SSE_Consumer starts, THE SSE_Consumer SHALL open an HTTP connection to `https://stream.wikimedia.org/v2/stream/recentchange` and begin receiving Server-Sent Events.
2. WHILE the SSE connection is open, THE SSE_Consumer SHALL parse each incoming SSE data line as a JSON Wikipedia_Event using Jackson.
3. WHEN the SSE_Consumer receives a Wikipedia_Event, THE SSE_Consumer SHALL extract the fields: title, wiki, user, bot, timestamp, type, comment, length (old and new), namespace, and revision.
4. IF the SSE connection drops unexpectedly, THEN THE SSE_Consumer SHALL log the disconnection at WARN level and initiate Reconnection_Backoff.

### Requirement 2: Namespace Filtering

**User Story:** As a data analyst, I want only main article edits to enter the pipeline, so that signals are not polluted by talk pages, user pages, or administrative edits.

#### Acceptance Criteria

1. WHEN the SSE_Consumer receives a Wikipedia_Event, THE SSE_Consumer SHALL check the namespace field of the event.
2. WHEN the namespace field equals 0, THE SSE_Consumer SHALL pass the Wikipedia_Event to the Kafka_Producer for publishing.
3. WHEN the namespace field does not equal 0, THE SSE_Consumer SHALL discard the Wikipedia_Event without publishing.

### Requirement 3: Kafka Event Production

**User Story:** As a downstream consumer, I want filtered Wikipedia events reliably published to Kafka, so that stream processors can consume them independently.

#### Acceptance Criteria

1. WHEN the Kafka_Producer receives a filtered Wikipedia_Event, THE Kafka_Producer SHALL serialize the event as JSON and send it to the `wikipedia.recentchanges` topic.
2. THE Kafka_Producer SHALL use the Wikipedia_Event title field as the Kafka message key to ensure edits to the same article land on the same partition.
3. WHEN the Kafka broker acknowledges a message, THE Kafka_Producer SHALL consider that event successfully delivered.
4. IF the Kafka broker rejects a message or the send times out, THEN THE Kafka_Producer SHALL log the failure at ERROR level and increment a failed-send counter.

### Requirement 4: SSE Reconnection with Exponential Backoff

**User Story:** As a system operator, I want the SSE consumer to automatically recover from disconnections without manual intervention, so that the pipeline remains operational during transient network issues.

#### Acceptance Criteria

1. WHEN the SSE connection fails, THE SSE_Consumer SHALL wait for an initial backoff delay of 1 second before the first reconnection attempt.
2. WHEN a reconnection attempt fails, THE SSE_Consumer SHALL double the backoff delay for the next attempt.
3. WHILE reconnecting, THE SSE_Consumer SHALL cap the backoff delay at a maximum of 30 seconds.
4. WHEN a reconnection attempt succeeds, THE SSE_Consumer SHALL reset the backoff delay to the initial value of 1 second.
5. WHILE the SSE_Consumer is in a reconnection cycle, THE SSE_Consumer SHALL log each attempt at INFO level with the current backoff delay.

### Requirement 5: Trending Article Signal Detection

**User Story:** As a data analyst, I want to know when an article is being edited unusually frequently, so that I can identify breaking news or emerging events.

#### Acceptance Criteria

1. THE Signal_Detector SHALL consume events from the `wikipedia.recentchanges` topic and group them by the title field.
2. THE Signal_Detector SHALL count edits per article within a configurable tumbling time window (default: 5 minutes).
3. WHEN the edit count for a single article within one window exceeds a configurable threshold (default: 5 edits), THE Signal_Detector SHALL emit a Trending_Signal to the `wikipedia.signals` topic.
4. THE Signal_Detector SHALL include in the Trending_Signal: the article title, the edit count, the window start timestamp, and the window end timestamp.

### Requirement 6: Bot Anomaly Signal Detection

**User Story:** As a system operator, I want to detect when bots account for an unusually high proportion of edits, so that I can identify bot misbehavior or coordinated automation.

#### Acceptance Criteria

1. THE Signal_Detector SHALL count the total number of edits and the number of bot edits (where the bot field is true) within a configurable tumbling time window (default: 5 minutes).
2. WHEN the ratio of bot edits to total edits within one window exceeds a configurable threshold (default: 0.8), THE Signal_Detector SHALL emit a Bot_Anomaly_Signal to the `wikipedia.signals` topic.
3. THE Signal_Detector SHALL include in the Bot_Anomaly_Signal: the bot-edit count, the total-edit count, the ratio, the window start timestamp, and the window end timestamp.
4. WHILE the total edit count within a window is fewer than 10, THE Signal_Detector SHALL suppress Bot_Anomaly_Signal emission to avoid false positives from low-volume periods.

### Requirement 7: Signal Persistence to DynamoDB

**User Story:** As a data analyst, I want detected signals stored durably, so that I can review historical patterns and trending events.

#### Acceptance Criteria

1. WHEN a signal appears on the `wikipedia.signals` topic, THE Persistence_Layer SHALL write the signal to a DynamoDB table.
2. THE Persistence_Layer SHALL store each signal with a partition key composed of the signal type and a sort key composed of the window end timestamp.
3. IF a DynamoDB write fails, THEN THE Persistence_Layer SHALL retry the write up to 3 times with exponential backoff before logging the failure at ERROR level.

### Requirement 8: Web Dashboard Signal Display

**User Story:** As a system operator, I want to view detected signals on a web page served by the application, so that I can monitor Wikipedia activity without relying on external notification services.

#### Acceptance Criteria

1. WHEN the Web_Dashboard starts, THE Web_Dashboard SHALL bind an embedded HTTP server to a configurable port (default: 8080) and begin serving requests.
2. THE Web_Dashboard SHALL serve an HTML page at the root path (`/`) that displays the most recent detected signals (up to the last 50 signals).
3. THE Web_Dashboard SHALL display each signal with: the signal type (Trending or Bot Anomaly), relevant metrics (edit count for Trending_Signal, bot ratio for Bot_Anomaly_Signal), article title (for Trending_Signals), and the time window (start and end timestamps).
4. THE Web_Dashboard SHALL provide an endpoint (default: `/api/signals`) that returns the recent signals as a JSON array for client-side polling.
5. THE Web_Dashboard HTML page SHALL automatically poll the JSON endpoint at a configurable interval (default: every 10 seconds) and refresh the displayed signal list without a full page reload.
6. IF the Web_Dashboard fails to bind to the configured port, THEN THE Web_Dashboard SHALL log the failure at ERROR level with the port number and terminate gracefully.

### Requirement 9: Phase Independence

**User Story:** As a developer, I want each phase to be independently runnable and testable, so that I can develop, test, and demo phases without depending on later phases.

#### Acceptance Criteria

1. THE SSE_Consumer and Kafka_Producer (Phase 1) SHALL run as a standalone process without requiring Phase 2 or Phase 3 components.
2. THE Signal_Detector (Phase 2) SHALL run as a standalone process that consumes from `wikipedia.recentchanges` without requiring Phase 1 to be running simultaneously (pre-populated topic data is sufficient).
3. THE Persistence_Layer and Web_Dashboard (Phase 3) SHALL run as a standalone process that consumes from `wikipedia.signals` without requiring Phase 2 to be running simultaneously.

### Requirement 10: Observability and Logging

**User Story:** As a system operator, I want structured logging throughout the pipeline, so that I can diagnose issues and monitor system health.

#### Acceptance Criteria

1. WHEN the SSE_Consumer connects successfully, THE SSE_Consumer SHALL log the connection at INFO level with the stream URL.
2. WHEN the Kafka_Producer successfully delivers a batch of events, THE Kafka_Producer SHALL log the count of events produced at DEBUG level.
3. WHEN the Signal_Detector emits a signal, THE Signal_Detector SHALL log the signal type, article title (if applicable), and key metrics at INFO level.
4. IF any component encounters an unrecoverable error, THEN that component SHALL log the error at ERROR level with the exception details and terminate gracefully.
