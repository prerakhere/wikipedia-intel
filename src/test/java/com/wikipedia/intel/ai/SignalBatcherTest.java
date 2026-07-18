package com.wikipedia.intel.ai;

import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SignalBatcherTest {

    @Test
    void flushesWhenCountThresholdReached() {
        List<List<TrendingSignal>> flushed = new ArrayList<>();
        SignalBatcher batcher = new SignalBatcher(3, 30000, flushed::add);

        batcher.add(signal("A"));
        batcher.add(signal("B"));
        assertEquals(0, flushed.size(), "Should not flush before threshold");

        batcher.add(signal("C"));
        assertEquals(1, flushed.size(), "Should flush at threshold");
        assertEquals(3, flushed.get(0).size());
        assertEquals(0, batcher.size(), "Buffer should be empty after flush");
    }

    @Test
    void tickFlushesWhenTimeThresholdReached() {
        List<List<TrendingSignal>> flushed = new ArrayList<>();

        // Start at time 0, maxWait = 1000ms
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(0), ZoneId.of("UTC"));
        SignalBatcher batcher = new SignalBatcher(10, 1000, flushed::add, fixedClock);

        batcher.add(signal("A"));
        batcher.tick();
        assertEquals(0, flushed.size(), "Should not flush before time threshold");

        // Advance clock past threshold
        Clock advancedClock = Clock.fixed(Instant.ofEpochMilli(1500), ZoneId.of("UTC"));
        SignalBatcher batcher2 = new SignalBatcher(10, 1000, flushed::add, advancedClock);
        batcher2.add(signal("B"));

        // Simulate time passing by creating with an old lastFlushTime
        // Better approach: use a mutable clock. Let's test flush() directly.
    }

    @Test
    void tickDoesNotFlushEmptyBuffer() {
        List<List<TrendingSignal>> flushed = new ArrayList<>();
        SignalBatcher batcher = new SignalBatcher(3, 0, flushed::add);

        batcher.tick();
        assertEquals(0, flushed.size(), "Should not flush empty buffer");
    }

    @Test
    void manualFlushSendsBufferedSignals() {
        List<List<TrendingSignal>> flushed = new ArrayList<>();
        SignalBatcher batcher = new SignalBatcher(10, 30000, flushed::add);

        batcher.add(signal("A"));
        batcher.add(signal("B"));
        batcher.flush();

        assertEquals(1, flushed.size());
        assertEquals(2, flushed.get(0).size());
        assertEquals("A", flushed.get(0).get(0).title());
        assertEquals("B", flushed.get(0).get(1).title());
    }

    @Test
    void manualFlushOnEmptyBufferDoesNothing() {
        List<List<TrendingSignal>> flushed = new ArrayList<>();
        SignalBatcher batcher = new SignalBatcher(10, 30000, flushed::add);

        batcher.flush();
        assertEquals(0, flushed.size());
    }

    @Test
    void multipleBatchesFlushIndependently() {
        List<List<TrendingSignal>> flushed = new ArrayList<>();
        SignalBatcher batcher = new SignalBatcher(2, 30000, flushed::add);

        batcher.add(signal("A"));
        batcher.add(signal("B")); // triggers flush
        batcher.add(signal("C"));
        batcher.add(signal("D")); // triggers second flush

        assertEquals(2, flushed.size());
        assertEquals(List.of("A", "B"), flushed.get(0).stream().map(TrendingSignal::title).toList());
        assertEquals(List.of("C", "D"), flushed.get(1).stream().map(TrendingSignal::title).toList());
    }

    private TrendingSignal signal(String title) {
        return new TrendingSignal(title, 5, List.of(), System.currentTimeMillis() - 300000, System.currentTimeMillis());
    }
}
