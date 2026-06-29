package com.wikipedia.intel.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Signal types: TrendingSignal and BotAnomalySignal.
 * Validates JSON polymorphic serialization/deserialization via @JsonTypeInfo.
 */
class SignalTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
    }

    @Test
    void trendingSignal_serialization_includesSignalTypeDiscriminator() throws JsonProcessingException {
        TrendingSignal signal = new TrendingSignal("Java_(programming_language)", 12, 1700000000L, 1700000300L);

        String json = mapper.writeValueAsString(signal);

        assertTrue(json.contains("\"signalType\":\"TRENDING\""),
                "Serialized JSON should contain signalType discriminator field with value TRENDING");
    }

    @Test
    void botAnomalySignal_serialization_includesSignalTypeDiscriminator() throws JsonProcessingException {
        BotAnomalySignal signal = new BotAnomalySignal(9, 10, 0.9, 1700000000L, 1700000300L);

        String json = mapper.writeValueAsString(signal);

        assertTrue(json.contains("\"signalType\":\"BOT_ANOMALY\""),
                "Serialized JSON should contain signalType discriminator field with value BOT_ANOMALY");
    }

    @Test
    void trendingSignal_deserialization_fromJsonWithDiscriminator() throws JsonProcessingException {
        String json = """
                {
                    "signalType": "TRENDING",
                    "title": "Kotlin_(programming_language)",
                    "editCount": 7,
                    "windowStart": 1700000000,
                    "windowEnd": 1700000300
                }
                """;

        Signal signal = mapper.readValue(json, Signal.class);

        assertInstanceOf(TrendingSignal.class, signal);
        TrendingSignal trending = (TrendingSignal) signal;
        assertEquals("Kotlin_(programming_language)", trending.title());
        assertEquals(7, trending.editCount());
        assertEquals(1700000000L, trending.windowStart());
        assertEquals(1700000300L, trending.windowEnd());
    }

    @Test
    void botAnomalySignal_deserialization_fromJsonWithDiscriminator() throws JsonProcessingException {
        String json = """
                {
                    "signalType": "BOT_ANOMALY",
                    "botEditCount": 15,
                    "totalEditCount": 18,
                    "ratio": 0.833,
                    "windowStart": 1700001000,
                    "windowEnd": 1700001300
                }
                """;

        Signal signal = mapper.readValue(json, Signal.class);

        assertInstanceOf(BotAnomalySignal.class, signal);
        BotAnomalySignal botAnomaly = (BotAnomalySignal) signal;
        assertEquals(15, botAnomaly.botEditCount());
        assertEquals(18, botAnomaly.totalEditCount());
        assertEquals(0.833, botAnomaly.ratio(), 0.001);
        assertEquals(1700001000L, botAnomaly.windowStart());
        assertEquals(1700001300L, botAnomaly.windowEnd());
    }

    @Test
    void trendingSignal_roundTrip_serializeDeserializeProducesEqualObject() throws JsonProcessingException {
        TrendingSignal original = new TrendingSignal("Main_Page", 25, 1700005000L, 1700005300L);

        String json = mapper.writeValueAsString(original);
        Signal deserialized = mapper.readValue(json, Signal.class);

        assertInstanceOf(TrendingSignal.class, deserialized);
        assertEquals(original, deserialized);
    }

    @Test
    void botAnomalySignal_roundTrip_serializeDeserializeProducesEqualObject() throws JsonProcessingException {
        BotAnomalySignal original = new BotAnomalySignal(42, 50, 0.84, 1700010000L, 1700010300L);

        String json = mapper.writeValueAsString(original);
        Signal deserialized = mapper.readValue(json, Signal.class);

        assertInstanceOf(BotAnomalySignal.class, deserialized);
        assertEquals(original, deserialized);
    }

    @Test
    void trendingSignal_signalType_returnsTrending() {
        TrendingSignal signal = new TrendingSignal("Test_Article", 5, 1000L, 2000L);
        assertEquals("TRENDING", signal.signalType());
    }

    @Test
    void botAnomalySignal_signalType_returnsBotAnomaly() {
        BotAnomalySignal signal = new BotAnomalySignal(8, 10, 0.8, 1000L, 2000L);
        assertEquals("BOT_ANOMALY", signal.signalType());
    }
}
