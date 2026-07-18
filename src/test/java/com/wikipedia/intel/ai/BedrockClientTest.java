package com.wikipedia.intel.ai;

import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class BedrockClientTest {

    private BedrockClient bedrockClient;

    @BeforeEach
    void setUp() {
        // Mock the AWS client — we only test prompt/parse logic here
        bedrockClient = new BedrockClient(mock(software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient.class));
    }

    @Test
    void buildPrompt_includesAllSignalTitlesAndEditCounts() {
        List<TrendingSignal> signals = List.of(
                new TrendingSignal("Infosys", 9, List.of(), 1000L, 2000L),
                new TrendingSignal("Angela Rayner", 6, List.of(), 1000L, 2000L)
        );

        String prompt = bedrockClient.buildPrompt(signals);

        assertTrue(prompt.contains("\"Infosys\" (9 edits in 5 minutes)"));
        assertTrue(prompt.contains("\"Angela Rayner\" (6 edits in 5 minutes)"));
        assertTrue(prompt.contains("event_type"));
        assertTrue(prompt.contains("JSON array"));
    }

    @Test
    void buildPrompt_includesAllEventTypes() {
        List<TrendingSignal> signals = List.of(new TrendingSignal("Test", 5, List.of(), 1000L, 2000L));
        String prompt = bedrockClient.buildPrompt(signals);

        assertTrue(prompt.contains("politics"));
        assertTrue(prompt.contains("sports"));
        assertTrue(prompt.contains("corporate"));
        assertTrue(prompt.contains("maintenance"));
    }

    @Test
    void buildPrompt_includesEditCommentsWhenPresent() {
        List<TrendingSignal> signals = List.of(
                new TrendingSignal("Zail Singh", 7,
                        List.of("/* Early life */ added citation", "Cleanup"),
                        1000L, 2000L)
        );

        String prompt = bedrockClient.buildPrompt(signals);

        assertTrue(prompt.contains("Edit comments:"));
        assertTrue(prompt.contains("/* Early life */ added citation"));
        assertTrue(prompt.contains("Cleanup"));
        assertTrue(prompt.contains("Do NOT speculate about deaths"));
    }

    @Test
    void parseResponse_validJsonArray_returnsEnrichedSignals() {
        List<TrendingSignal> signals = List.of(
                new TrendingSignal("Infosys", 9, List.of(), 1000L, 2000L),
                new TrendingSignal("Angela Rayner", 6, List.of(), 3000L, 4000L)
        );

        String response = """
                [
                  {"event_type": "corporate", "summary": "Quarterly earnings released", "confidence": 0.85, "attention_level": "high"},
                  {"event_type": "politics", "summary": "UK political controversy", "confidence": 0.72, "attention_level": "medium"}
                ]
                """;

        List<EnrichedSignal> enriched = bedrockClient.parseResponse(response, signals);

        assertEquals(2, enriched.size());

        assertEquals("Infosys", enriched.get(0).title());
        assertEquals("corporate", enriched.get(0).eventType());
        assertEquals("Quarterly earnings released", enriched.get(0).summary());
        assertEquals(0.85, enriched.get(0).confidence(), 0.001);
        assertEquals("high", enriched.get(0).attentionLevel());
        assertEquals(9, enriched.get(0).editCount());
        assertEquals(1000L, enriched.get(0).windowStart());

        assertEquals("Angela Rayner", enriched.get(1).title());
        assertEquals("politics", enriched.get(1).eventType());
        assertEquals(0.72, enriched.get(1).confidence(), 0.001);
    }

    @Test
    void parseResponse_withMarkdownCodeFences_stripsThemAndParses() {
        List<TrendingSignal> signals = List.of(
                new TrendingSignal("Test_Article", 5, List.of(), 1000L, 2000L)
        );

        String response = """
                ```json
                [{"event_type": "other", "summary": "General editing", "confidence": 0.5, "attention_level": "low"}]
                ```
                """;

        List<EnrichedSignal> enriched = bedrockClient.parseResponse(response, signals);

        assertEquals(1, enriched.size());
        assertEquals("other", enriched.get(0).eventType());
    }

    @Test
    void parseResponse_invalidJson_returnsEmptyList() {
        List<TrendingSignal> signals = List.of(
                new TrendingSignal("Test", 5, List.of(), 1000L, 2000L)
        );

        List<EnrichedSignal> enriched = bedrockClient.parseResponse("not json at all", signals);

        assertEquals(0, enriched.size());
    }

    @Test
    void parseResponse_fewerItemsThanSignals_returnsPartialResults() {
        List<TrendingSignal> signals = List.of(
                new TrendingSignal("A", 5, List.of(), 1000L, 2000L),
                new TrendingSignal("B", 7, List.of(), 1000L, 2000L),
                new TrendingSignal("C", 3, List.of(), 1000L, 2000L)
        );

        String response = """
                [
                  {"event_type": "sports", "summary": "Match result", "confidence": 0.9, "attention_level": "high"}
                ]
                """;

        List<EnrichedSignal> enriched = bedrockClient.parseResponse(response, signals);

        assertEquals(1, enriched.size());
        assertEquals("A", enriched.get(0).title());
    }

    @Test
    void parseResponse_missingFields_usesDefaults() {
        List<TrendingSignal> signals = List.of(
                new TrendingSignal("Test", 5, List.of(), 1000L, 2000L)
        );

        String response = """
                [{"event_type": "politics"}]
                """;

        List<EnrichedSignal> enriched = bedrockClient.parseResponse(response, signals);

        assertEquals(1, enriched.size());
        assertEquals("politics", enriched.get(0).eventType());
        assertEquals("No explanation available", enriched.get(0).summary());
        assertEquals(0.5, enriched.get(0).confidence(), 0.001);
        assertEquals("medium", enriched.get(0).attentionLevel());
    }
}
