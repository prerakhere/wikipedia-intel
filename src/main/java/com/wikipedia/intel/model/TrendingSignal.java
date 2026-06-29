package com.wikipedia.intel.model;

/**
 * Signal indicating an article is trending — it received more edits than the
 * configured threshold within a single tumbling window.
 */
public record TrendingSignal(
    String title,
    int editCount,
    long windowStart,
    long windowEnd
) implements Signal {

    @Override
    public String signalType() {
        return "TRENDING";
    }
}
