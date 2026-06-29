package com.wikipedia.intel.sse;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SseReconnector exponential backoff computation.
 * TDD: written before the implementation exists.
 */
class SseReconnectorTest {

    private final SseReconnector reconnector = new SseReconnector(1000L, 30000L);

    @Test
    void computeDelay_attempt0_returnsInitialDelay() {
        assertEquals(1000L, reconnector.computeDelay(0));
    }

    @Test
    void computeDelay_attempt1_doublesInitialDelay() {
        assertEquals(2000L, reconnector.computeDelay(1));
    }

    @Test
    void computeDelay_attempt2_quadruplesInitialDelay() {
        assertEquals(4000L, reconnector.computeDelay(2));
    }

    @Test
    void computeDelay_attempt3_returns8000ms() {
        assertEquals(8000L, reconnector.computeDelay(3));
    }

    @Test
    void computeDelay_attempt4_returns16000ms_stillUnderCap() {
        assertEquals(16000L, reconnector.computeDelay(4));
    }

    @Test
    void computeDelay_attempt5_cappedAtMaxDelay() {
        // 1000 * 2^5 = 32000, but max is 30000
        assertEquals(30000L, reconnector.computeDelay(5));
    }

    @Test
    void computeDelay_highAttempt_remainsCapped() {
        assertEquals(30000L, reconnector.computeDelay(10));
    }

    @Test
    void initialDelay_returnsConfiguredValue() {
        assertEquals(1000L, reconnector.initialDelay());
    }
}
