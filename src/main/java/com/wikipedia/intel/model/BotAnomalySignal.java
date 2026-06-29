package com.wikipedia.intel.model;

/**
 * Signal indicating an anomalous ratio of bot edits within a tumbling window.
 * Emitted when botEditCount/totalEditCount exceeds the configured threshold
 * and totalEditCount meets the minimum volume requirement.
 */
public record BotAnomalySignal(
    int botEditCount,
    int totalEditCount,
    double ratio,
    long windowStart,
    long windowEnd
) implements Signal {

    @Override
    public String signalType() {
        return "BOT_ANOMALY";
    }
}
