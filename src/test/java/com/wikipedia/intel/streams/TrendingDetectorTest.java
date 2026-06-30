package com.wikipedia.intel.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TrendingDetector using TopologyTestDriver.
 * Validates windowed edit-count aggregation and threshold-based signal emission.
 *
 * <p>TrendingDetector groups events by article title in a tumbling window,
 * counts edits, and emits a TrendingSignal when the count exceeds the threshold.
 */
class TrendingDetectorTest {

    private static final String INPUT_TOPIC = "wikipedia.recentchanges";
    private static final String OUTPUT_TOPIC = "wikipedia.signals";
    private static final int THRESHOLD = 5;
    private static final long WINDOW_MINUTES = 5;

    private TopologyTestDriver driver;
    private TestInputTopic<String, WikipediaEvent> inputTopic;
    private TestOutputTopic<String, Signal> outputTopic;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();

        TrendingDetector detector = new TrendingDetector(THRESHOLD, WINDOW_MINUTES);

        StreamsBuilder builder = new StreamsBuilder();
        Serde<WikipediaEvent> eventSerde = buildJsonSerde(WikipediaEvent.class);
        Serde<Signal> signalSerde = buildJsonSerde(Signal.class);

        KStream<String, WikipediaEvent> source = builder.stream(
                INPUT_TOPIC,
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), eventSerde));

        KStream<String, Signal> signals = detector.buildStream(source);
        signals.to(OUTPUT_TOPIC,
                org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), signalSerde));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trending-detector-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        driver = new TopologyTestDriver(builder.build(), props);

        inputTopic = driver.createInputTopic(
                INPUT_TOPIC,
                Serdes.String().serializer(),
                eventSerde.serializer());

        outputTopic = driver.createOutputTopic(
                OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                signalSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void fourEditsToSameArticle_noSignalEmitted() {
        String title = "Java_(programming_language)";
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // Send 4 edits within the same 5-minute window — below threshold
        for (int i = 0; i < 4; i++) {
            inputTopic.pipeInput(title, createEvent(title), baseTime.plus(Duration.ofSeconds(i * 30)));
        }

        // Advance time past window boundary to flush results
        inputTopic.pipeInput(title, createEvent(title),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        // The window with 4 events should NOT produce a signal (threshold is 5)
        // The new event starts a new window, so we check signals emitted for the closed window
        var records = outputTopic.readRecordsToList();
        boolean hasSignalForClosedWindow = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .anyMatch(ts -> ts.editCount() == 4);

        assertFalse(hasSignalForClosedWindow,
                "4 edits (below threshold of 5) should NOT produce a TrendingSignal");
    }

    @Test
    void fiveEditsToSameArticle_trendingSignalEmitted() {
        String title = "Kafka_(software)";
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // Send 5 edits within the same 5-minute window — meets threshold
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(title, createEvent(title), baseTime.plus(Duration.ofSeconds(i * 30)));
        }

        // Advance time past window boundary to flush the window
        inputTopic.pipeInput("Other_Article", createEvent("Other_Article"),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        boolean hasSignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .anyMatch(ts -> ts.title().equals(title) && ts.editCount() >= 5);

        assertTrue(hasSignal,
                "5+ edits (meeting threshold) should produce a TrendingSignal");
    }

    @Test
    void sixEditsToSameArticle_trendingSignalEmitted() {
        String title = "Apache_Kafka";
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // Send 6 edits — above threshold
        for (int i = 0; i < 6; i++) {
            inputTopic.pipeInput(title, createEvent(title), baseTime.plus(Duration.ofSeconds(i * 20)));
        }

        // Advance time past window boundary
        inputTopic.pipeInput("Filler_Article", createEvent("Filler_Article"),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        boolean hasSignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .anyMatch(ts -> ts.title().equals(title) && ts.editCount() == 6);

        assertTrue(hasSignal,
                "6 edits (above threshold) should produce a TrendingSignal with editCount=6");
    }

    @Test
    void trendingSignal_containsCorrectFields() {
        String title = "Stream_processing";
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // Send exactly 5 edits
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(title, createEvent(title), baseTime.plus(Duration.ofSeconds(i * 10)));
        }

        // Advance time past window boundary to flush
        inputTopic.pipeInput("Flush_Event", createEvent("Flush_Event"),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        TrendingSignal signal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .filter(ts -> ts.title().equals(title))
                .findFirst()
                .orElse(null);

        assertNotNull(signal, "Expected a TrendingSignal for article: " + title);
        assertEquals(title, signal.title(), "Signal title should match the article");
        assertEquals(5, signal.editCount(), "Signal editCount should equal number of edits in window");
        assertTrue(signal.windowStart() > 0, "windowStart should be a positive timestamp");
        assertTrue(signal.windowEnd() > signal.windowStart(), "windowEnd should be after windowStart");

        // The window should be 5 minutes (300000ms) wide
        long windowDuration = signal.windowEnd() - signal.windowStart();
        assertEquals(Duration.ofMinutes(WINDOW_MINUTES).toMillis(), windowDuration,
                "Window duration should be exactly 5 minutes");
    }

    @Test
    void differentArticles_eachTrackedIndependently() {
        String titleA = "Article_A";
        String titleB = "Article_B";
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // Article A gets 5 edits (should trigger), Article B gets 3 (should not)
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(titleA, createEvent(titleA), baseTime.plus(Duration.ofSeconds(i * 10)));
        }
        for (int i = 0; i < 3; i++) {
            inputTopic.pipeInput(titleB, createEvent(titleB), baseTime.plus(Duration.ofSeconds(i * 10)));
        }

        // Flush the window
        inputTopic.pipeInput("Flush", createEvent("Flush"),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        long signalsForA = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .filter(ts -> ts.title().equals(titleA))
                .count();

        long signalsForB = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .filter(ts -> ts.title().equals(titleB))
                .count();

        assertTrue(signalsForA >= 1, "Article A with 5 edits should produce a signal");
        assertEquals(0, signalsForB, "Article B with 3 edits should NOT produce a signal");
    }

    // --- Helper methods ---

    /**
     * Creates a WikipediaEvent for the given article title.
     * Uses sensible defaults for all other fields.
     */
    private WikipediaEvent createEvent(String title) {
        return new WikipediaEvent(
                title,
                "enwiki",
                "TestUser",
                false,
                System.currentTimeMillis() / 1000,
                "edit",
                "test edit",
                0,
                new WikipediaEvent.Revision(100, 101),
                new WikipediaEvent.Length(500, 520)
        );
    }

    /**
     * Builds a JSON Serde for the given type using Jackson.
     * Used for WikipediaEvent and Signal serialization in the test topology.
     */
    private <T> Serde<T> buildJsonSerde(Class<T> type) {
        Serializer<T> serializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return mapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize " + type.getSimpleName(), e);
            }
        };

        Deserializer<T> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return mapper.readValue(bytes, type);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize " + type.getSimpleName(), e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
