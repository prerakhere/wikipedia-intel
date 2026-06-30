package com.wikipedia.intel.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BotAnomalyDetector using TopologyTestDriver.
 * Validates windowed bot-ratio aggregation, threshold filtering, and minimum volume guard.
 *
 * <p>BotAnomalyDetector groups ALL events by a constant key, tracks total and bot edit counts
 * in a tumbling window, computes the ratio, and emits a BotAnomalySignal when ratio > threshold
 * AND totalEditCount >= minimumVolume.
 */
class BotAnomalyDetectorTest {

    private static final String INPUT_TOPIC = "wikipedia.recentchanges";
    private static final String OUTPUT_TOPIC = "wikipedia.signals";
    private static final double RATIO_THRESHOLD = 0.8;
    private static final int MINIMUM_VOLUME = 10;
    private static final long WINDOW_MINUTES = 5;

    private TopologyTestDriver driver;
    private TestInputTopic<String, WikipediaEvent> inputTopic;
    private TestOutputTopic<String, Signal> outputTopic;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();

        BotAnomalyDetector detector = new BotAnomalyDetector(RATIO_THRESHOLD, MINIMUM_VOLUME, WINDOW_MINUTES);

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bot-anomaly-detector-test");
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
    @DisplayName("8 bot edits + 2 human edits (ratio 0.8, total 10) → no signal (not > 0.8)")
    void ratioAtThreshold_noSignalEmitted() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // 8 bot edits
        for (int i = 0; i < 8; i++) {
            inputTopic.pipeInput("key", createEvent("Article", true),
                    baseTime.plus(Duration.ofSeconds(i * 10)));
        }
        // 2 human edits
        for (int i = 0; i < 2; i++) {
            inputTopic.pipeInput("key", createEvent("Article", false),
                    baseTime.plus(Duration.ofSeconds((8 + i) * 10)));
        }

        // Advance past 5-minute window to trigger closure
        inputTopic.pipeInput("key", createEvent("Flush", false),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        boolean hasBotSignalForClosedWindow = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof BotAnomalySignal)
                .map(s -> (BotAnomalySignal) s)
                .anyMatch(bas -> bas.totalEditCount() == 10 && bas.botEditCount() == 8);

        assertFalse(hasBotSignalForClosedWindow,
                "Ratio 0.8 (not strictly > 0.8) should NOT emit a BotAnomalySignal");
    }

    @Test
    @DisplayName("9 bot edits + 1 human edit (ratio 0.9, total 10) → BotAnomalySignal emitted")
    void ratioAboveThreshold_signalEmitted() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // 9 bot edits
        for (int i = 0; i < 9; i++) {
            inputTopic.pipeInput("key", createEvent("Article", true),
                    baseTime.plus(Duration.ofSeconds(i * 10)));
        }
        // 1 human edit
        inputTopic.pipeInput("key", createEvent("Article", false),
                baseTime.plus(Duration.ofSeconds(90)));

        // Advance past 5-minute window to trigger closure
        inputTopic.pipeInput("key", createEvent("Flush", false),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        boolean hasSignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof BotAnomalySignal)
                .map(s -> (BotAnomalySignal) s)
                .anyMatch(bas -> bas.botEditCount() == 9 && bas.totalEditCount() == 10);

        assertTrue(hasSignal,
                "Ratio 0.9 (> 0.8) with volume 10 should emit a BotAnomalySignal");
    }

    @Test
    @DisplayName("5 bot edits + 0 human edits (ratio 1.0, total 5) → suppressed (volume < 10)")
    void lowVolume_suppressesSignal() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // 5 bot edits only — total volume = 5, which is < minimumVolume of 10
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput("key", createEvent("Article", true),
                    baseTime.plus(Duration.ofSeconds(i * 10)));
        }

        // Advance past 5-minute window to trigger closure
        inputTopic.pipeInput("key", createEvent("Flush", false),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        boolean hasBotSignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof BotAnomalySignal)
                .map(s -> (BotAnomalySignal) s)
                .anyMatch(bas -> bas.totalEditCount() == 5);

        assertFalse(hasBotSignal,
                "Volume 5 (< minimumVolume 10) should suppress signal even with ratio 1.0");
    }

    @Test
    @DisplayName("Signal contains correct botEditCount, totalEditCount, ratio, and window timestamps")
    void signalContainsCorrectFields() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // 10 bot edits + 2 human edits = total 12, ratio = 10/12 ≈ 0.833 (> 0.8)
        for (int i = 0; i < 10; i++) {
            inputTopic.pipeInput("key", createEvent("Article", true),
                    baseTime.plus(Duration.ofSeconds(i * 10)));
        }
        for (int i = 0; i < 2; i++) {
            inputTopic.pipeInput("key", createEvent("Article", false),
                    baseTime.plus(Duration.ofSeconds((10 + i) * 10)));
        }

        // Advance past the window to flush
        inputTopic.pipeInput("key", createEvent("Flush", false),
                baseTime.plus(Duration.ofMinutes(WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        BotAnomalySignal signal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof BotAnomalySignal)
                .map(s -> (BotAnomalySignal) s)
                .filter(bas -> bas.totalEditCount() == 12)
                .findFirst()
                .orElse(null);

        assertNotNull(signal, "Expected a BotAnomalySignal for ratio 0.833 with volume 12");
        assertEquals(10, signal.botEditCount(), "botEditCount should be 10");
        assertEquals(12, signal.totalEditCount(), "totalEditCount should be 12");
        assertEquals(10.0 / 12.0, signal.ratio(), 0.001, "ratio should be ~0.833");
        assertTrue(signal.windowStart() > 0, "windowStart should be a positive timestamp");
        assertTrue(signal.windowEnd() > signal.windowStart(), "windowEnd should be after windowStart");

        // Window duration should be exactly 5 minutes (300000ms)
        long windowDuration = signal.windowEnd() - signal.windowStart();
        assertEquals(Duration.ofMinutes(WINDOW_MINUTES).toMillis(), windowDuration,
                "Window duration should be exactly 5 minutes");
    }

    // --- Helper methods ---

    /**
     * Creates a WikipediaEvent with the given title and bot flag.
     * All events use namespace=0 (main namespace).
     */
    private WikipediaEvent createEvent(String title, boolean bot) {
        return new WikipediaEvent(
                title,
                "enwiki",
                bot ? "BotUser" : "HumanUser",
                bot,
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
