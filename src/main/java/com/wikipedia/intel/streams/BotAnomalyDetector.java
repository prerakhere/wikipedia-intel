package com.wikipedia.intel.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;

/**
 * Detects bot anomalies by tracking the ratio of bot edits to total edits
 * within a tumbling window. Emits a {@link BotAnomalySignal} when the bot ratio
 * exceeds the configured threshold AND total edit volume meets the minimum.
 *
 * <p>All events are grouped by a constant key so that the entire stream is aggregated
 * into a single window, producing a global bot-ratio metric per window.
 */
public class BotAnomalyDetector {

    private static final String GLOBAL_KEY = "global";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final double ratioThreshold;
    private final int minimumVolume;
    private final long windowMinutes;

    /**
     * Creates a BotAnomalyDetector with the given configuration.
     *
     * @param ratioThreshold bot ratio must be strictly greater than this to trigger a signal
     * @param minimumVolume  minimum total edit count required before ratio is evaluated
     * @param windowMinutes  size of the tumbling window in minutes
     */
    public BotAnomalyDetector(double ratioThreshold, int minimumVolume, long windowMinutes) {
        this.ratioThreshold = ratioThreshold;
        this.minimumVolume = minimumVolume;
        this.windowMinutes = windowMinutes;
    }

    /**
     * Builds the bot anomaly detection stream from the given source of WikipediaEvents.
     *
     * <p>The stream is rekeyed to a constant key so all events land in the same group,
     * then windowed and aggregated into total/bot counts. A signal is emitted only when
     * the ratio exceeds the threshold and the volume meets the minimum.
     *
     * @param source input stream of WikipediaEvents
     * @return stream of Signal instances (BotAnomalySignal) for windows exceeding thresholds
     */
    public KStream<String, Signal> buildStream(KStream<String, WikipediaEvent> source) {
        Serde<BotCounts> botCountsSerde = buildBotCountsSerde();
        Serde<WikipediaEvent> eventSerde = buildEventSerde();

        return source
                .selectKey((key, value) -> GLOBAL_KEY)
                .groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .aggregate(
                        BotCounts::empty,
                        (key, event, counts) -> counts.add(event.bot()),
                        Materialized.with(Serdes.String(), botCountsSerde)
                )
                .toStream()
                .filter((windowedKey, counts) ->
                        counts.total() >= minimumVolume
                                && counts.ratio() > ratioThreshold)
                .map((Windowed<String> windowedKey, BotCounts counts) -> {
                    long windowStart = windowedKey.window().start();
                    long windowEnd = windowedKey.window().end();
                    Signal signal = new BotAnomalySignal(
                            counts.bot(),
                            counts.total(),
                            counts.ratio(),
                            windowStart,
                            windowEnd
                    );
                    return KeyValue.pair(GLOBAL_KEY, signal);
                });
    }

    /**
     * Simple aggregate state holding total and bot edit counts for a window.
     */
    record BotCounts(int total, int bot) {

        static BotCounts empty() {
            return new BotCounts(0, 0);
        }

        BotCounts add(boolean isBot) {
            return new BotCounts(total + 1, isBot ? bot + 1 : bot);
        }

        double ratio() {
            if (total == 0) return 0.0;
            return (double) bot / total;
        }
    }

    /**
     * Builds a JSON-based Serde for WikipediaEvent using Jackson.
     */
    private Serde<WikipediaEvent> buildEventSerde() {
        Serializer<WikipediaEvent> serializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize WikipediaEvent", e);
            }
        };

        Deserializer<WikipediaEvent> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readValue(bytes, WikipediaEvent.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize WikipediaEvent", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }

    /**
     * Builds a JSON-based Serde for BotCounts using Jackson.
     */
    private Serde<BotCounts> buildBotCountsSerde() {
        Serializer<BotCounts> serializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize BotCounts", e);
            }
        };

        Deserializer<BotCounts> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readValue(bytes, BotCounts.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize BotCounts", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
