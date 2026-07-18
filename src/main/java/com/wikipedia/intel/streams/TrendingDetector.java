package com.wikipedia.intel.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Detects trending articles by counting edits per article within a tumbling window.
 * Also collects recent edit comments for AI enrichment context.
 * Emits a {@link TrendingSignal} when the edit count exceeds the configured threshold.
 */
public class TrendingDetector {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_COMMENTS = 5;

    private final int threshold;
    private final long windowMinutes;

    public TrendingDetector(int threshold, long windowMinutes) {
        this.threshold = threshold;
        this.windowMinutes = windowMinutes;
    }

    /**
     * Builds the trending detection stream. Groups by article title, applies a tumbling window,
     * aggregates edit count + comments, filters on threshold, and maps to TrendingSignal.
     */
    public KStream<String, Signal> buildStream(KStream<String, WikipediaEvent> source) {
        Serde<EditAggregate> aggregateSerde = buildAggregateSerde();

        return source
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .aggregate(
                        EditAggregate::empty,
                        (key, event, agg) -> agg.add(event.comment()),
                        Materialized.with(Serdes.String(), aggregateSerde)
                )
                .toStream()
                .filter((windowedKey, agg) -> agg.count() >= threshold)
                .map((Windowed<String> windowedKey, EditAggregate agg) -> {
                    String title = windowedKey.key();
                    long windowStart = windowedKey.window().start();
                    long windowEnd = windowedKey.window().end();
                    Signal signal = new TrendingSignal(title, agg.count(), agg.comments(),
                            windowStart, windowEnd);
                    return KeyValue.pair(title, signal);
                });
    }

    /**
     * Aggregate state: edit count + recent comments (capped at MAX_COMMENTS).
     */
    public record EditAggregate(int count, List<String> comments) {

        static EditAggregate empty() {
            return new EditAggregate(0, List.of());
        }

        EditAggregate add(String comment) {
            List<String> newComments = new ArrayList<>(comments);
            if (comment != null && !comment.isBlank() && newComments.size() < MAX_COMMENTS) {
                newComments.add(comment);
            }
            return new EditAggregate(count + 1, newComments);
        }
    }

    private Serde<EditAggregate> buildAggregateSerde() {
        Serializer<EditAggregate> serializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize EditAggregate", e);
            }
        };

        Deserializer<EditAggregate> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readValue(bytes, EditAggregate.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize EditAggregate", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
