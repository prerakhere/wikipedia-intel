package com.wikipedia.intel.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for SignalTopology — the complete Kafka Streams topology
 * that wires both TrendingDetector and BotAnomalyDetector into a single topology.
 *
 * <p>Verifies end-to-end behavior: events piped into the input topic produce the
 * correct signals on the output topic, with both detection branches operating
 * independently without interference.
 */
class SignalTopologyTest {

    private static final String INPUT_TOPIC = "wikipedia.recentchanges";
    private static final String OUTPUT_TOPIC = "wikipedia.signals";

    // Trending config
    private static final int TRENDING_THRESHOLD = 5;
    private static final long TRENDING_WINDOW_MINUTES = 5;

    // Bot anomaly config
    private static final double BOT_RATIO_THRESHOLD = 0.8;
    private static final int BOT_MINIMUM_VOLUME = 10;
    private static final long BOT_WINDOW_MINUTES = 5;

    private TopologyTestDriver driver;
    private TestInputTopic<String, WikipediaEvent> inputTopic;
    private TestOutputTopic<String, Signal> outputTopic;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();

        SignalTopology topology = new SignalTopology(
                INPUT_TOPIC, OUTPUT_TOPIC,
                TRENDING_THRESHOLD, TRENDING_WINDOW_MINUTES,
                BOT_RATIO_THRESHOLD, BOT_MINIMUM_VOLUME, BOT_WINDOW_MINUTES);

        Serde<WikipediaEvent> eventSerde = buildJsonSerde(WikipediaEvent.class);
        Serde<Signal> signalSerde = buildJsonSerde(Signal.class);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "signal-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        driver = new TopologyTestDriver(topology.build(), props);

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
    @DisplayName("6 edits to same article in one window → TrendingSignal appears on output topic")
    void trendingBranch_emitsSignalAboveThreshold() {
        String title = "Kafka_(software)";
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // Send 6 edits to the same article within the trending window
        for (int i = 0; i < 6; i++) {
            inputTopic.pipeInput(title, createEvent(title, false),
                    baseTime.plus(Duration.ofSeconds(i * 20)));
        }

        // Advance past window boundary to flush results
        inputTopic.pipeInput("Flush_Article", createEvent("Flush_Article", false),
                baseTime.plus(Duration.ofMinutes(TRENDING_WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        boolean hasTrendingSignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .anyMatch(ts -> ts.title().equals(title) && ts.editCount() == 6);

        assertTrue(hasTrendingSignal,
                "6 edits (above threshold of 5) should produce a TrendingSignal on the output topic");
    }

    @Test
    @DisplayName("9 bot + 1 human edit in one window → BotAnomalySignal appears on output topic")
    void botAnomalyBranch_emitsSignalAboveThreshold() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // 9 bot edits
        for (int i = 0; i < 9; i++) {
            inputTopic.pipeInput("Article_" + i, createEvent("Article_" + i, true),
                    baseTime.plus(Duration.ofSeconds(i * 10)));
        }
        // 1 human edit
        inputTopic.pipeInput("Article_9", createEvent("Article_9", false),
                baseTime.plus(Duration.ofSeconds(90)));

        // Advance past window boundary to flush results
        inputTopic.pipeInput("Flush", createEvent("Flush", false),
                baseTime.plus(Duration.ofMinutes(BOT_WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();
        boolean hasBotAnomalySignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof BotAnomalySignal)
                .map(s -> (BotAnomalySignal) s)
                .anyMatch(bas -> bas.botEditCount() == 9 && bas.totalEditCount() == 10);

        assertTrue(hasBotAnomalySignal,
                "9 bot + 1 human (ratio 0.9 > 0.8, volume 10) should produce a BotAnomalySignal");
    }

    @Test
    @DisplayName("Both trending and bot anomaly signals appear in the same test run")
    void bothBranches_coexistWithoutInterference() {
        String trendingArticle = "Trending_Article";
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // Trigger trending: 6 edits to the same article (all from bots to also contribute to bot branch)
        for (int i = 0; i < 6; i++) {
            inputTopic.pipeInput(trendingArticle, createEvent(trendingArticle, true),
                    baseTime.plus(Duration.ofSeconds(i * 10)));
        }

        // Add more bot edits (different articles) to bring total volume to 10+ with ratio > 0.8
        for (int i = 0; i < 4; i++) {
            inputTopic.pipeInput("Other_" + i, createEvent("Other_" + i, true),
                    baseTime.plus(Duration.ofSeconds((6 + i) * 10)));
        }
        // 1 human edit — now total = 11, bot = 10, ratio = 10/11 ≈ 0.909
        inputTopic.pipeInput("Human_Edit", createEvent("Human_Edit", false),
                baseTime.plus(Duration.ofSeconds(100)));

        // Advance past window boundary to flush both branches
        inputTopic.pipeInput("Flush", createEvent("Flush", false),
                baseTime.plus(Duration.ofMinutes(TRENDING_WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();

        boolean hasTrendingSignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .anyMatch(ts -> ts.title().equals(trendingArticle) && ts.editCount() >= 6);

        boolean hasBotAnomalySignal = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof BotAnomalySignal)
                .map(s -> (BotAnomalySignal) s)
                .anyMatch(bas -> bas.ratio() > BOT_RATIO_THRESHOLD
                        && bas.totalEditCount() >= BOT_MINIMUM_VOLUME);

        assertTrue(hasTrendingSignal,
                "Trending branch should emit a signal for article with 6+ edits");
        assertTrue(hasBotAnomalySignal,
                "Bot anomaly branch should emit a signal for ratio > 0.8 with volume >= 10");
    }

    @Test
    @DisplayName("Events below both thresholds produce no signals")
    void belowBothThresholds_noSignals() {
        Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");

        // 3 edits to same article — below trending threshold of 5
        for (int i = 0; i < 3; i++) {
            inputTopic.pipeInput("Low_Edit_Article", createEvent("Low_Edit_Article", false),
                    baseTime.plus(Duration.ofSeconds(i * 10)));
        }
        // 2 bot edits + 3 human edits (total = 5, volume < 10) — below bot minimum volume
        for (int i = 0; i < 2; i++) {
            inputTopic.pipeInput("Bot_Article_" + i, createEvent("Bot_Article_" + i, true),
                    baseTime.plus(Duration.ofSeconds((3 + i) * 10)));
        }

        // Advance past window boundary to flush
        inputTopic.pipeInput("Flush", createEvent("Flush", false),
                baseTime.plus(Duration.ofMinutes(TRENDING_WINDOW_MINUTES).plusMillis(1)));

        var records = outputTopic.readRecordsToList();

        // Filter out any signals from the flush event's window (the flush starts a new window)
        long trendingCount = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof TrendingSignal)
                .map(s -> (TrendingSignal) s)
                .filter(ts -> !ts.title().equals("Flush"))
                .count();

        long botAnomalyCount = records.stream()
                .map(r -> r.value())
                .filter(s -> s instanceof BotAnomalySignal)
                .map(s -> (BotAnomalySignal) s)
                .filter(bas -> bas.totalEditCount() <= 5)
                .count();

        assertEquals(0, trendingCount,
                "3 edits (below threshold of 5) should NOT produce a TrendingSignal");
        assertEquals(0, botAnomalyCount,
                "Total volume 5 (below minimum of 10) should NOT produce a BotAnomalySignal");
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
     * Handles polymorphic deserialization for Signal types via Jackson's @JsonTypeInfo.
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
