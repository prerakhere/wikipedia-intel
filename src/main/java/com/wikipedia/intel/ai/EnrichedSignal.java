package com.wikipedia.intel.ai;

/**
 * A signal enriched with AI-generated classification and interpretation.
 * Produced by the Bedrock enrichment layer from raw TrendingSignal data.
 */
public record EnrichedSignal(
    String title,
    int editCount,
    String eventType,
    String summary,
    double confidence,
    String attentionLevel,
    long windowStart,
    long windowEnd
) {

    /**
     * Valid event types for classification.
     */
    public static final String[] EVENT_TYPES = {
        "politics", "sports", "corporate", "death", "disaster",
        "controversy", "entertainment", "science", "maintenance", "other"
    };

    /**
     * Valid attention levels.
     */
    public static final String[] ATTENTION_LEVELS = {"high", "medium", "low"};
}
