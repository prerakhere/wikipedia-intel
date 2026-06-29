package com.wikipedia.intel.sse;

/**
 * Manages exponential backoff for SSE reconnection.
 * Thread-safe, stateless computation — backoff state is passed in/out.
 */
public class SseReconnector {

    private final long initialDelayMs;
    private final long maxDelayMs;

    public SseReconnector(long initialDelayMs, long maxDelayMs) {
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    /**
     * Computes the next backoff delay given the current attempt number.
     * Uses exponential backoff: initialDelay * 2^attempt, capped at maxDelay.
     *
     * @param attempt zero-based attempt number (0 = first failure)
     * @return delay in milliseconds, capped at maxDelayMs
     */
    public long computeDelay(int attempt) {
        return Math.min(initialDelayMs * (1L << attempt), maxDelayMs);
    }

    /**
     * Returns the initial delay (used after successful reconnection to reset state).
     */
    public long initialDelay() {
        return initialDelayMs;
    }
}
