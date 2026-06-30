package com.wikipedia.intel.persist;

import com.wikipedia.intel.model.Signal;

/**
 * Derives DynamoDB partition key and sort key from a Signal.
 * Partition key groups signals by type; sort key orders them by window end time.
 */
public class DynamoKeyStrategy {

    /**
     * Returns the partition key for a signal — the signal type string
     * (e.g., "TRENDING" or "BOT_ANOMALY").
     */
    public String partitionKey(Signal signal) {
        return signal.signalType();
    }

    /**
     * Returns the sort key for a signal — the window end timestamp as a string
     * (epoch millis). This gives lexicographic ordering that matches chronological order.
     */
    public String sortKey(Signal signal) {
        return String.valueOf(signal.windowEnd());
    }
}
