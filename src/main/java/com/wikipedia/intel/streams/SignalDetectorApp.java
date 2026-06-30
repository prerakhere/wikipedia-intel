package com.wikipedia.intel.streams;

import com.wikipedia.intel.config.PipelineConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Phase 2 entry point: runs the Kafka Streams signal detector.
 * Consumes from the wikipedia.recentchanges topic, detects trending articles
 * and bot anomalies via windowed aggregations, and emits signals to wikipedia.signals.
 */
public class SignalDetectorApp {

    private static final Logger log = LoggerFactory.getLogger(SignalDetectorApp.class);

    public static void main(String[] args) {
        PipelineConfig config = PipelineConfig.load();

        SignalTopology signalTopology = new SignalTopology(
                config.inputTopic(), config.signalsTopic(),
                config.trendingThreshold(), config.trendingWindowMinutes(),
                config.botRatioThreshold(), config.botMinimumVolume(), config.botWindowMinutes()
        );

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikipedia-signal-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());

        KafkaStreams streams = new KafkaStreams(signalTopology.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down signal detector");
            streams.close();
        }));

        log.info("Starting signal detector (trending threshold={}, bot ratio threshold={})",
                config.trendingThreshold(), config.botRatioThreshold());
        streams.start();
    }
}
