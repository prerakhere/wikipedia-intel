package com.wikipedia.intel.ai;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.config.PipelineConfig;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import com.wikipedia.intel.web.DashboardServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Phase 4 entry point — consumes signals from Kafka, enriches them via Bedrock (Nova Micro),
 * and serves an enriched dashboard on port 8081.
 *
 * <p>Runs as a standalone JVM process via {@code ./gradlew runPhase4}.
 */
public class AiEnrichmentApp {

    private static final Logger log = LoggerFactory.getLogger(AiEnrichmentApp.class);
    private static final int ENRICHED_DASHBOARD_PORT = 8081;
    private static final int BATCH_SIZE = 10;
    private static final long BATCH_WAIT_MS = 30_000; // 30 seconds

    public static void main(String[] args) {
        PipelineConfig config = PipelineConfig.load();
        ObjectMapper mapper = new ObjectMapper();

        // Bedrock client
        BedrockRuntimeClient bedrockRuntimeClient = BedrockRuntimeClient.builder()
                .region(Region.of(config.bedrockRegion()))
                .build();
        BedrockClient bedrockClient = new BedrockClient(bedrockRuntimeClient);

        // Enriched signals store (thread-safe, rolling)
        CopyOnWriteArrayList<EnrichedSignal> enrichedSignals = new CopyOnWriteArrayList<>();

        // Enriched dashboard server
        EnrichedDashboardHandler dashboardHandler = new EnrichedDashboardHandler(enrichedSignals);
        DashboardServer server;
        try {
            server = new DashboardServer(ENRICHED_DASHBOARD_PORT, dashboardHandler);
        } catch (IOException e) {
            log.error("Failed to start enriched dashboard on port {}", ENRICHED_DASHBOARD_PORT, e);
            return;
        }
        server.start();

        // Signal batcher → Bedrock
        SignalBatcher batcher = new SignalBatcher(BATCH_SIZE, BATCH_WAIT_MS, batch -> {
            log.info("Enriching batch of {} signals via Bedrock", batch.size());
            List<EnrichedSignal> enriched = bedrockClient.enrich(batch);
            if (!enriched.isEmpty()) {
                enrichedSignals.addAll(enriched);
                // Trim to last 50 enriched signals
                while (enrichedSignals.size() > 50) {
                    enrichedSignals.remove(0);
                }
                log.info("Enriched {} signals (total: {})", enriched.size(), enrichedSignals.size());
            }
        });

        // Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-ai-enrichment");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(config.signalsTopic()));

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down AiEnrichmentApp...");
            server.stop();
            consumer.wakeup();
        }));

        log.info("AI Enrichment running — dashboard at http://localhost:{}", ENRICHED_DASHBOARD_PORT);

        // Consumer loop
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    try {
                        Signal signal = mapper.readValue(record.value(), Signal.class);
                        if (signal instanceof TrendingSignal trending) {
                            batcher.add(trending);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse signal: {}", e.getMessage());
                    }
                }
                batcher.tick(); // Check time-based flush
            }
        } catch (WakeupException e) {
            log.info("Consumer wakeup received, shutting down");
            batcher.flush(); // Flush remaining
        } finally {
            consumer.close();
            log.info("AiEnrichmentApp stopped");
        }
    }
}
