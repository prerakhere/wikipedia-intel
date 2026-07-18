package com.wikipedia.intel.ai;

import com.wikipedia.intel.model.TrendingSignal;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Accumulates TrendingSignal instances and flushes them as a batch
 * when either the count threshold or time threshold is reached.
 *
 * <p>Designed to be called from a single-threaded Kafka consumer loop.
 */
public class SignalBatcher {

    private final int maxBatchSize;
    private final long maxWaitMs;
    private final Consumer<List<TrendingSignal>> onFlush;
    private final Clock clock;

    private final List<TrendingSignal> buffer = new ArrayList<>();
    private long lastFlushTime;

    /**
     * Creates a batcher with the given thresholds.
     *
     * @param maxBatchSize flush when this many signals are buffered
     * @param maxWaitMs    flush if this many milliseconds have passed since last flush
     * @param onFlush      callback invoked with the batch when flushing
     */
    public SignalBatcher(int maxBatchSize, long maxWaitMs, Consumer<List<TrendingSignal>> onFlush) {
        this(maxBatchSize, maxWaitMs, onFlush, Clock.systemUTC());
    }

    /**
     * Constructor with injectable clock for testing.
     */
    SignalBatcher(int maxBatchSize, long maxWaitMs, Consumer<List<TrendingSignal>> onFlush, Clock clock) {
        this.maxBatchSize = maxBatchSize;
        this.maxWaitMs = maxWaitMs;
        this.onFlush = onFlush;
        this.clock = clock;
        this.lastFlushTime = clock.millis();
    }

    /**
     * Adds a signal to the buffer. Flushes if count threshold is reached.
     */
    public void add(TrendingSignal signal) {
        buffer.add(signal);
        if (buffer.size() >= maxBatchSize) {
            flush();
        }
    }

    /**
     * Checks if the time threshold has been reached and flushes if so.
     * Should be called periodically (e.g., after each consumer poll).
     */
    public void tick() {
        if (!buffer.isEmpty() && (clock.millis() - lastFlushTime) >= maxWaitMs) {
            flush();
        }
    }

    /**
     * Forces a flush of whatever is in the buffer, regardless of thresholds.
     */
    public void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<TrendingSignal> batch = new ArrayList<>(buffer);
        buffer.clear();
        lastFlushTime = clock.millis();
        onFlush.accept(batch);
    }

    /**
     * Returns the current number of buffered signals.
     */
    public int size() {
        return buffer.size();
    }
}
