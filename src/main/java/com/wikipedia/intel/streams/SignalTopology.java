package com.wikipedia.intel.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Builds the Kafka Streams topology for signal detection.
 * Encapsulates both trending and bot-anomaly detection in a single topology,
 * reading from a source topic and writing merged signals to an output topic.
 */
public class SignalTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String inputTopic;
    private final String outputTopic;
    private final int trendingThreshold;
    private final long trendingWindowMinutes;
    private final double botRatioThreshold;
    private final int botMinimumVolume;
    private final long botWindowMinutes;

    /**
     * Creates a SignalTopology with the given configuration.
     *
     * @param inputTopic           source topic containing WikipediaEvent records
     * @param outputTopic          sink topic for emitted Signal records
     * @param trendingThreshold    minimum edit count to trigger a trending signal
     * @param trendingWindowMinutes tumbling window size for trending detection
     * @param botRatioThreshold    bot ratio must exceed this to trigger a bot anomaly signal
     * @param botMinimumVolume     minimum total edits before bot ratio is evaluated
     * @param botWindowMinutes     tumbling window size for bot anomaly detection
     */
    public SignalTopology(String inputTopic, String outputTopic,
                          int trendingThreshold, long trendingWindowMinutes,
                          double botRatioThreshold, int botMinimumVolume, long botWindowMinutes) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.trendingThreshold = trendingThreshold;
        this.trendingWindowMinutes = trendingWindowMinutes;
        this.botRatioThreshold = botRatioThreshold;
        this.botMinimumVolume = botMinimumVolume;
        this.botWindowMinutes = botWindowMinutes;
    }

    /**
     * Builds and returns the complete detection topology.
     * Both TrendingDetector and BotAnomalyDetector consume from the same source stream,
     * and their output signals are merged into a single stream written to the output topic.
     *
     * @return the constructed Kafka Streams Topology
     */
    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<WikipediaEvent> eventSerde = buildJsonSerde(WikipediaEvent.class);
        Serde<Signal> signalSerde = buildJsonSerde(Signal.class);

        // Source stream: read WikipediaEvents from the input topic keyed by article title
        KStream<String, WikipediaEvent> source = builder.stream(
                inputTopic, Consumed.with(Serdes.String(), eventSerde));

        // Trending detection branch
        TrendingDetector trendingDetector = new TrendingDetector(trendingThreshold, trendingWindowMinutes);
        KStream<String, Signal> trendingSignals = trendingDetector.buildStream(source);

        // Bot anomaly detection branch
        BotAnomalyDetector botAnomalyDetector = new BotAnomalyDetector(
                botRatioThreshold, botMinimumVolume, botWindowMinutes);
        KStream<String, Signal> botAnomalySignals = botAnomalyDetector.buildStream(source);

        // Merge both signal streams and write to the output topic
        trendingSignals.merge(botAnomalySignals)
                .to(outputTopic, Produced.with(Serdes.String(), signalSerde));

        return builder.build();
    }

    /**
     * Builds a JSON-based Serde for the given type using Jackson.
     * Handles polymorphic serialization/deserialization for Signal types.
     */
    private <T> Serde<T> buildJsonSerde(Class<T> type) {
        Serializer<T> serializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize " + type.getSimpleName(), e);
            }
        };

        Deserializer<T> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readValue(bytes, type);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize " + type.getSimpleName(), e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
