package com.wikipedia.intel.ai;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EnrichedSignalTest {

    @Test
    void recordFieldsAreAccessible() {
        EnrichedSignal signal = new EnrichedSignal(
                "Infosys", 9, "corporate",
                "Likely quarterly earnings released",
                0.85, "high",
                1700000000000L, 1700000300000L);

        assertEquals("Infosys", signal.title());
        assertEquals(9, signal.editCount());
        assertEquals("corporate", signal.eventType());
        assertEquals("Likely quarterly earnings released", signal.summary());
        assertEquals(0.85, signal.confidence(), 0.001);
        assertEquals("high", signal.attentionLevel());
    }
}
