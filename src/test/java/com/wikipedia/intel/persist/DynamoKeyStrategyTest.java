package com.wikipedia.intel.persist;

import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for DynamoKeyStrategy — verifies partition key and sort key derivation
 * from Signal instances.
 */
class DynamoKeyStrategyTest {

    private final DynamoKeyStrategy strategy = new DynamoKeyStrategy();

    @Test
    void partitionKey_returnsTrending_forTrendingSignal() {
        var signal = new TrendingSignal("Java_(programming_language)", 7, 1000L, 2000L);
        assertEquals("TRENDING", strategy.partitionKey(signal));
    }

    @Test
    void partitionKey_returnsBotAnomaly_forBotAnomalySignal() {
        var signal = new BotAnomalySignal(9, 10, 0.9, 3000L, 4000L);
        assertEquals("BOT_ANOMALY", strategy.partitionKey(signal));
    }

    @Test
    void sortKey_returnsWindowEndTimestamp_forTrendingSignal() {
        var signal = new TrendingSignal("Kafka", 12, 1000L, 1_700_000_000_000L);
        assertEquals("1700000000000", strategy.sortKey(signal));
    }

    @Test
    void sortKey_returnsWindowEndTimestamp_forBotAnomalySignal() {
        var signal = new BotAnomalySignal(15, 18, 0.83, 5000L, 1_700_500_000_000L);
        assertEquals("1700500000000", strategy.sortKey(signal));
    }
}
