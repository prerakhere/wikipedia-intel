package com.wikipedia.intel.ai;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.TrendingSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Invokes Amazon Bedrock (Nova Micro) to classify and explain trending Wikipedia signals.
 * Sends a batch of signals in a single prompt and parses the structured JSON response.
 */
public class BedrockClient {

    private static final Logger log = LoggerFactory.getLogger(BedrockClient.class);
    private static final String MODEL_ID = "apac.amazon.nova-micro-v1:0";

    private final BedrockRuntimeClient client;
    private final ObjectMapper mapper;

    public BedrockClient(BedrockRuntimeClient client) {
        this.client = client;
        this.mapper = new ObjectMapper();
    }

    /**
     * Enriches a batch of trending signals by calling Bedrock.
     * Returns enriched signals for each input; on failure returns an empty list.
     */
    public List<EnrichedSignal> enrich(List<TrendingSignal> signals) {
        if (signals.isEmpty()) {
            return List.of();
        }

        String prompt = buildPrompt(signals);
        String responseText = invokeModel(prompt);
        if (responseText == null) {
            return List.of();
        }

        return parseResponse(responseText, signals);
    }

    /**
     * Builds the classification prompt for a batch of signals.
     * Package-private for testability.
     */
    String buildPrompt(List<TrendingSignal> signals) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a Wikipedia activity analyst. Classify each trending article below based on its title and recent edit comments.\n\n");
        sb.append("Rules:\n");
        sb.append("- Base your classification ONLY on the title and edit comments provided.\n");
        sb.append("- If the edit comments suggest routine maintenance (fixing links, adding categories, cleanup), classify as 'maintenance' with LOW attention.\n");
        sb.append("- If you cannot determine why an article is trending from the available information, use event_type 'other', set confidence below 0.4, and say 'Reason unclear from available edit context' in summary.\n");
        sb.append("- Do NOT speculate about deaths, disasters, or breaking news unless the edit comments clearly indicate it.\n");
        sb.append("- Large byte additions (+1000) suggest substantial content being added. Small or net-zero changes suggest minor edits or reverts.\n\n");
        sb.append("For each article, provide a JSON object with:\n");
        sb.append("- event_type: one of [politics, sports, corporate, death, disaster, controversy, entertainment, science, maintenance, other]\n");
        sb.append("- summary: one sentence explaining what the edits appear to be about\n");
        sb.append("- confidence: 0.0-1.0 (how confident you are in the classification)\n");
        sb.append("- attention_level: one of [high, medium, low]\n\n");
        sb.append("Respond with ONLY a JSON array. One object per article in the same order.\n\n");
        sb.append("Articles:\n");

        for (int i = 0; i < signals.size(); i++) {
            TrendingSignal s = signals.get(i);
            sb.append(String.format("%d. \"%s\" (%d edits in 5 minutes, net %+d bytes)\n",
                    i + 1, s.title(), s.editCount(), s.totalBytesChanged()));
            if (s.recentComments() != null && !s.recentComments().isEmpty()) {
                sb.append("   Edit comments:\n");
                for (String comment : s.recentComments()) {
                    sb.append(String.format("   - %s\n", comment));
                }
            }
        }

        return sb.toString();
    }

    /**
     * Calls Bedrock InvokeModel and returns the text response.
     * Returns null on failure.
     */
    private String invokeModel(String prompt) {
        try {
            String requestBody = mapper.writeValueAsString(Map.of(
                    "messages", List.of(Map.of("role", "user", "content", List.of(Map.of("text", prompt)))),
                    "inferenceConfig", Map.of("maxTokens", 1024, "temperature", 0.2)
            ));

            InvokeModelRequest request = InvokeModelRequest.builder()
                    .modelId(MODEL_ID)
                    .contentType("application/json")
                    .accept("application/json")
                    .body(SdkBytes.fromUtf8String(requestBody))
                    .build();

            InvokeModelResponse response = client.invokeModel(request);
            String responseJson = response.body().asUtf8String();

            // Parse the response to extract the text content
            Map<String, Object> responseMap = mapper.readValue(responseJson, new TypeReference<>() {});
            @SuppressWarnings("unchecked")
            var output = (Map<String, Object>) responseMap.get("output");
            @SuppressWarnings("unchecked")
            var message = (Map<String, Object>) output.get("message");
            @SuppressWarnings("unchecked")
            var content = (List<Map<String, Object>>) message.get("content");
            return (String) content.get(0).get("text");
        } catch (Exception e) {
            log.error("Bedrock invocation failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Parses the model's JSON array response into EnrichedSignal instances.
     * Falls back gracefully if parsing fails for individual items.
     */
    List<EnrichedSignal> parseResponse(String responseText, List<TrendingSignal> originalSignals) {
        List<EnrichedSignal> results = new ArrayList<>();

        try {
            // Strip markdown code fences if present
            String json = responseText.strip();
            if (json.startsWith("```")) {
                json = json.replaceAll("^```[a-z]*\\n?", "").replaceAll("\\n?```$", "").strip();
            }

            List<Map<String, Object>> items = mapper.readValue(json, new TypeReference<>() {});

            for (int i = 0; i < Math.min(items.size(), originalSignals.size()); i++) {
                Map<String, Object> item = items.get(i);
                TrendingSignal original = originalSignals.get(i);

                String eventType = getStringOrDefault(item, "event_type", "other");
                String summary = getStringOrDefault(item, "summary", "No explanation available");
                double confidence = getDoubleOrDefault(item, "confidence", 0.5);
                String attentionLevel = getStringOrDefault(item, "attention_level", "medium");

                results.add(new EnrichedSignal(
                        original.title(), original.editCount(),
                        eventType, summary, confidence, attentionLevel,
                        original.windowStart(), original.windowEnd()
                ));
            }
        } catch (Exception e) {
            log.warn("Failed to parse Bedrock response: {}", e.getMessage());
        }

        return results;
    }

    private String getStringOrDefault(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value instanceof String s ? s : defaultValue;
    }

    private double getDoubleOrDefault(Map<String, Object> map, String key, double defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number n) return n.doubleValue();
        if (value instanceof String s) {
            try { return Double.parseDouble(s); } catch (NumberFormatException e) { /* fall through */ }
        }
        return defaultValue;
    }
}
