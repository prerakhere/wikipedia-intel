# Project Conventions — wikipedia-intel

## Tech Stack (non-negotiable)

- Java 21, Gradle 8.7 (use the wrapper: `./gradlew`)
- No Spring Boot — plain Java with `main()`. Kafka Streams apps don't need a web framework.
- Apache Kafka 3.7.0 (local Docker, KRaft mode, port 9092)
- Jackson 2.17.0 for JSON
- SLF4J + Logback for logging
- JUnit 5 for testing

## Development Methodology

- TDD: write tests first, then implement to make them pass
- Keep classes small and focused (single responsibility)
- Every new feature must have corresponding unit tests
- Integration tests for Kafka interactions (use embedded/test containers where appropriate)

## Kafka

- Local broker at `localhost:9092` via Docker Compose
- Topics: `wikipedia.recentchanges` (raw events), `wikipedia.signals` (detected patterns), `wikipedia.test` (smoke test)
- KRaft mode, no ZooKeeper

## Data Source

- Wikipedia EventStreams SSE: `https://stream.wikimedia.org/v2/stream/recentchange`
- Free, no auth, no rate limit
- Filter to namespace 0 only (main Wikipedia articles — not talk pages, user pages, or categories)

## Code Style

- Clean, production-quality code — no "AI-generated" looking comments
- Use meaningful variable/method names
- Javadoc on public classes and methods
- Package: `com.wikipedia.intel`

## Git & Repo

- Public repo — keep it professional
- Commit messages: concise, imperative mood
- Don't commit secrets, CONTEXT.md, or LEARNING.md (they're gitignored)
- **Atomic TDD commits**: After each TDD round (test + implementation passes), commit immediately. Do NOT continue to the next task without committing first. Pause and report progress to the user after each commit.

## Build Commands

```bash
./gradlew build          # Compile + test
./gradlew run            # Run App.java
./gradlew verifyKafka    # Kafka smoke test
./gradlew clean          # Clean build artifacts
docker compose up -d     # Start Kafka
docker compose down      # Stop Kafka
```
