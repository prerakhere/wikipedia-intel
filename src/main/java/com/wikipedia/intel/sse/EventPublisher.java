package com.wikipedia.intel.sse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Receives WikipediaEvents, applies namespace filter, serializes to JSON,
 * and produces to the wikipedia.recentchanges Kafka topic.
 */
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final String topic;
    private final AtomicLong failedSendCount = new AtomicLong(0);

    public EventPublisher(KafkaProducer<String, String> producer, ObjectMapper mapper, String topic) {
        this.producer = producer;
        this.mapper = mapper;
        this.topic = topic;
    }

    /**
     * Filters and publishes a single event.
     *
     * @return true if the event was published (namespace == 0), false if discarded
     */
    public boolean publish(WikipediaEvent event) {
        if (!event.isMainNamespace()) {
            return false;
        }

        String json;
        try {
            json = mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize event for title={}", event.title(), e);
            failedSendCount.incrementAndGet();
            return false;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.title(), json);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send event for title={}", event.title(), exception);
                failedSendCount.incrementAndGet();
            }
        });

        return true;
    }

    /**
     * Returns the count of failed sends since startup.
     */
    public long failedSendCount() {
        return failedSendCount.get();
    }
}
