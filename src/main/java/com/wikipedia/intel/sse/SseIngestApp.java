package com.wikipedia.intel.sse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.config.PipelineConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Phase 1 entry point: connects to Wikipedia SSE stream, publishes filtered
 * events to Kafka, and reconnects with exponential backoff on disconnection.
 */
public class SseIngestApp {

    private static final Logger log = LoggerFactory.getLogger(SseIngestApp.class);

    private final SseClient sseClient;
    private final EventPublisher publisher;
    private final SseReconnector reconnector;
    private volatile boolean running;

    public SseIngestApp(SseClient sseClient, EventPublisher publisher, SseReconnector reconnector) {
        this.sseClient = sseClient;
        this.publisher = publisher;
        this.reconnector = reconnector;
        this.running = true;
    }

    /**
     * Runs the reconnection loop, blocking until stopped.
     * On each iteration: connects to SSE, and when disconnected,
     * computes backoff delay and sleeps before reconnecting.
     */
    public void run() {
        log.info("SSE ingest app started");
        int attempt = 0;

        while (running) {
            log.info("Connecting to SSE...");
            sseClient.connect(event -> publisher.publish(event));

            if (!running) {
                break;
            }

            long delay = reconnector.computeDelay(attempt);
            log.info("SSE disconnected, reconnecting in {} ms (attempt {})", delay, attempt);

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!running) {
                    break;
                }
            }

            attempt++;
        }
    }

    /**
     * Signals the reconnection loop to exit and closes the SSE connection.
     */
    public void stop() {
        running = false;
        sseClient.close();
    }

    /**
     * Creates real dependencies from PipelineConfig and starts the ingest app.
     */
    public static void main(String[] args) {
        PipelineConfig config = PipelineConfig.load();

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        ObjectMapper mapper = new ObjectMapper();

        SseClient sseClient = new SseClient(config.sseUrl(), mapper);
        EventPublisher publisher = new EventPublisher(producer, mapper, config.inputTopic());
        SseReconnector reconnector = new SseReconnector(config.reconnectInitialMs(), config.reconnectMaxMs());

        SseIngestApp app = new SseIngestApp(sseClient, publisher, reconnector);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, stopping SSE ingest app");
            app.stop();
            producer.close();
        }));

        app.run();
    }
}
