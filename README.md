# Wikipedia Intel

A real-time streaming intelligence system that ingests Wikipedia's live edit stream, detects meaningful patterns using Kafka Streams, and enriches edit signals with AI classification via Amazon Bedrock.

## Architecture

```
┌──────────────────────┐
│  Wikipedia SSE Feed  │
│  (EventStreams API)  │
└──────────┬───────────┘
           │ HTTP/SSE
           ▼
┌──────────────────────┐         ┌──────────────────────────────────────┐
│   SSE Consumer       │         │            Apache Kafka              │
│   (Phase 1)          │────────►│                                      │
│                      │ produce │  ┌────────────────────────────────┐  │
│ • Connects to SSE    │         │  │  wikipedia.recentchanges       │  │
│ • Filters enwiki     │         │  │  (raw article edit events)     │  │
│ • Namespace 0 only   │         │  └───────────────┬────────────────┘  │
│ • Backoff reconnect  │         │                  │                   │
└──────────────────────┘         │                  │ consume           │
                                 │                  ▼                   │
                                 │  ┌────────────────────────────────┐  │
                                 │  │  Kafka Streams Signal Detector │  │
                                 │  │  (Phase 2)                     │  │
                                 │  │                                │  │
                                 │  │  • 5-min tumbling windows      │  │
                                 │  │  • TrendingDetector (≥5 edits) │  │
                                 │  │  • BotAnomalyDetector (>30%)   │  │
                                 │  │  • Aggregates: count, comments,│  │
                                 │  │    byte delta per article      │  │
                                 │  └───────────────┬────────────────┘  │
                                 │                  │ produce           │
                                 │                  ▼                   │
                                 │  ┌────────────────────────────────┐  │
                                 │  │  wikipedia.signals             │  │
                                 │  │  (detected trending + anomaly) │  │
                                 │  └──────┬─────────────────┬───────┘  │
                                 │         │                 │          │
                                 └─────────┼─────────────────┼──────────┘
                                           │                 │
                              consume      │                 │      consume
                          ┌────────────────┘                 └────────────────┐
                          ▼                                                   ▼
               ┌──────────────────────┐                        ┌──────────────────────────┐
               │   Raw Dashboard      │                        │   AI Enrichment          │
               │   (Phase 3)          │                        │   (Phase 4)              │
               │                      │                        │                          │
               │ • Sorted by edits    │                        │ • Batches signals        │
               │ • Rolling 10-min     │                        │ • Calls Amazon Bedrock   │
               │ • IST timestamps     │                        │ • Classifies event type  │
               │ • Wikipedia links    │                        │ • Generates summaries    │
               │                      │                        │ • Confidence scoring     │
               │   http://...:8080    │                        │                          │
               └──────────────────────┘                        │   http://...:8081        │
                                                               └─────────────┬────────────┘
                                                                             │
                                                                             │ InvokeModel
                                                                             ▼
                                                               ┌──────────────────────────┐
                                                               │   Amazon Bedrock         │
                                                               │   (Nova Micro)           │
                                                               │                          │
                                                               │ • Event classification   │
                                                               │ • Natural language        │
                                                               │   summary generation     │
                                                               │ • Confidence estimation  │
                                                               └──────────────────────────┘
```

## Signal Detection

The system detects two types of signals from the Wikipedia edit stream:

**Trending Articles** — articles receiving ≥5 edits within a 5-minute window. Each signal includes the article title, edit count, recent edit comments (up to 5), and net byte change.

**Bot Anomalies** — windows where bot edits exceed 30% of total edit volume (minimum 5 edits). Indicates unusual automated activity.

## AI Enrichment

Detected signals are batched and sent to Amazon Bedrock (Nova Micro) for classification. The model receives the article title, edit count, edit comments, and byte delta as context.

For each signal, the model returns:
- **Event type** — politics, sports, corporate, death, disaster, controversy, entertainment, science, maintenance, other
- **Summary** — one-sentence explanation of what the edits appear to be about
- **Confidence** — 0.0 to 1.0
- **Attention level** — high, medium, low

The prompt is designed to minimize hallucination: the model is instructed to classify based only on available evidence and to report "unknown" with low confidence when context is insufficient.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Java 21 |
| Build | Gradle 8.7 |
| Streaming | Apache Kafka 3.7.0, Kafka Streams |
| AI | Amazon Bedrock (Nova Micro) |
| Web | JDK built-in HTTP server |
| Data Source | Wikipedia EventStreams SSE |
| Containerization | Docker (Kafka in KRaft mode) |
| Testing | JUnit 5, Mockito, TopologyTestDriver |

## Running

```bash
# Start Kafka
docker compose up -d

# Start the pipeline (each phase is a separate process)
./gradlew runPhase1 &    # SSE consumer → Kafka
./gradlew runPhase2 &    # Kafka Streams signal detection
./gradlew runPhase3 &    # Raw signal dashboard (port 8080)

# AI enrichment (requires AWS credentials with Bedrock access)
AWS_PROFILE=<profile> ./gradlew runPhase4 &    # Enriched dashboard (port 8081)
```

**Prerequisites:**
- Java 21
- Docker
- AWS credentials with Bedrock access (for Phase 4 only)

## Design Decisions

- **No Spring Boot** — plain Java with `main()`. Kafka Streams applications don't need a web framework.
- **KRaft mode** — no ZooKeeper dependency, single-container Kafka setup.
- **Separate processes per phase** — each phase runs independently. Phases 1-3 work without AWS. Phase 4 is optional.
- **No persistence layer** — dashboards are live-only. Signals are ephemeral. Keeps the architecture focused on streaming.
- **Batched AI calls** — signals are accumulated (10 signals or 30 seconds) before calling Bedrock, reducing API overhead.
- **Anti-hallucination prompt design** — the model receives edit comments and byte deltas as evidence, and is explicitly instructed not to speculate beyond available context.
- **enwiki filter** — only English Wikipedia main-namespace articles are processed, filtering out Wikidata and non-English editions.

## Project Structure

```
src/main/java/com/wikipedia/intel/
├── config/          Configuration loading (env vars, system props, defaults)
├── model/           Domain records (WikipediaEvent, Signal, TrendingSignal, BotAnomalySignal)
├── sse/             Phase 1 — SSE connection, parsing, Kafka producing
├── streams/         Phase 2 — Kafka Streams topology, windowed aggregation
├── web/             Phase 3 — Dashboard server, signal formatting
└── ai/              Phase 4 — Bedrock client, batching, enriched dashboard
```

## Development Approach

- **Phases 1–3** (streaming pipeline) — built using [Kiro IDE's](https://kiro.dev) spec-driven development: requirements → design doc → task breakdown → TDD implementation. See `.kiro/specs/` for the full spec artifacts.
- **Phase 4** (AI enrichment) — vibe-coded iteratively in the terminal, with rapid prompt tuning against the live stream.

## Motivation

This project was built to learn Kafka Streams, windowed aggregations, and real-time data pipelines on a genuine high-volume live feed. The domain (Wikipedia edit trends) is incidental — the architecture maps directly to fraud detection, observability, IoT, ad-tech, or any event-driven system.
