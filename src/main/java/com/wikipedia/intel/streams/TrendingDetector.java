package com.wikipedia.intel.streams;

import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;

/**
 * Detects trending articles by counting edits per article within a tumbling window.
 * Emits a {@link TrendingSignal} when the edit count exceeds the configured threshold.
 */
public class TrendingDetector {

    private final int threshold;
    private final long windowMinutes;

    /**
     * Creates a TrendingDetector with the given threshold and window size.
     *
     * @param threshold    minimum edit count to trigger a trending signal
     * @param windowMinutes size of the tumbling window in minutes
     */
    public TrendingDetector(int threshold, long windowMinutes) {
        this.threshold = threshold;
        this.windowMinutes = windowMinutes;
    }

    /**
     * Builds the trending detection stream from the given source of WikipediaEvents.
     * Groups events by key (article title), applies a tumbling window, counts per window,
     * filters on the threshold, and maps to TrendingSignal.
     *
     * @param source input stream keyed by article title
     * @return stream of Signal instances (TrendingSignal) for articles exceeding the threshold
     */
    public KStream<String, Signal> buildStream(KStream<String, WikipediaEvent> source) {
        return source
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .filter((windowedKey, count) -> count >= threshold)
                .map((Windowed<String> windowedKey, Long count) -> {
                    String title = windowedKey.key();
                    long windowStart = windowedKey.window().start();
                    long windowEnd = windowedKey.window().end();
                    Signal signal = new TrendingSignal(title, count.intValue(), windowStart, windowEnd);
                    return KeyValue.pair(title, signal);
                });
    }
}
