package com.wikipedia.intel.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.config.PipelineConfig;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.persist.DynamoKeyStrategy;
import com.wikipedia.intel.persist.DynamoSignalWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Phase 3 entry point — consumes signals from Kafka, persists them to DynamoDB,
 * and serves them on a lightweight HTTP dashboard.
 *
 * <p>Runs as a standalone JVM process via {@code ./gradlew runPhase3}.
 */
public class DashboardApp {

    private static final Logger log = LoggerFactory.getLogger(DashboardApp.class);

    public static void main(String[] args) {
        PipelineConfig config = PipelineConfig.load();
        ObjectMapper mapper = new ObjectMapper();

        // DynamoDB client
        DynamoDbClient dynamoClient = DynamoDbClient.builder()
                .region(Region.of(config.dynamoRegion()))
                .build();
        DynamoKeyStrategy keyStrategy = new DynamoKeyStrategy();
        DynamoSignalWriter writer = new DynamoSignalWriter(dynamoClient, keyStrategy, config.dynamoTableName());

        // Web dashboard
        SignalFormatter formatter = new SignalFormatter();
        SignalHandler handler = new SignalHandler(formatter);
        DashboardServer server;
        try {
            server = new DashboardServer(config.dashboardPort(), handler);
        } catch (IOException e) {
            log.error("Failed to create dashboard server on port {}", config.dashboardPort(), e);
            return;
        }
        server.start();

        // Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-dashboard");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(config.signalsTopic()));

        // Shutdown hook for graceful close
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down DashboardApp...");
            server.stop();
            consumer.wakeup();
        }));

        log.info("Dashboard running at http://localhost:{}", config.dashboardPort());

        // Consumer loop
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    try {
                        Signal signal = mapper.readValue(record.value(), Signal.class);
                        handler.addSignal(signal);
                        writer.write(signal);
                    } catch (Exception e) {
                        log.warn("Failed to process signal record: {}", e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown — ignore
            log.info("Consumer wakeup received, shutting down");
        } finally {
            consumer.close();
            log.info("DashboardApp stopped");
        }
    }
}
