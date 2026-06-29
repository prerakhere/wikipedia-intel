package com.wikipedia.intel.sse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.WikipediaEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for EventPublisher.
 * Tests namespace filtering, Kafka message key assignment, and error tracking.
 */
class EventPublisherTest {

    private static final String TOPIC = "wikipedia.recentchanges";

    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;
    private EventPublisher publisher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        producer = mock(KafkaProducer.class);
        mapper = new ObjectMapper();
        publisher = new EventPublisher(producer, mapper, TOPIC);
    }

    @Test
    void publishWithNamespaceZeroReturnsTrueAndSendsToKafka() {
        WikipediaEvent event = createEvent("Test Article", 0);

        when(producer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenReturn(mock(Future.class));

        boolean result = publisher.publish(event);

        assertTrue(result, "publish() should return true for namespace=0 events");
        verify(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    void publishWithNonZeroNamespaceReturnsFalseAndDoesNotSend() {
        WikipediaEvent event = createEvent("Talk:Some Page", 1);

        boolean result = publisher.publish(event);

        assertFalse(result, "publish() should return false for namespace≠0 events");
        verify(producer, never()).send(any(ProducerRecord.class), any(Callback.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void publishedRecordKeyEqualsEventTitle() {
        String title = "Java (programming language)";
        WikipediaEvent event = createEvent(title, 0);

        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        when(producer.send(captor.capture(), any(Callback.class)))
                .thenReturn(mock(Future.class));

        publisher.publish(event);

        ProducerRecord<String, String> record = captor.getValue();
        assertEquals(title, record.key(), "Kafka message key should equal the event title");
        assertEquals(TOPIC, record.topic(), "Record should target the configured topic");
    }

    @SuppressWarnings("unchecked")
    @Test
    void failedSendIncreasesFailedSendCount() {
        WikipediaEvent event = createEvent("Some Article", 0);

        // Capture the callback so we can invoke it with an exception
        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        when(producer.send(any(ProducerRecord.class), callbackCaptor.capture()))
                .thenReturn(mock(Future.class));

        publisher.publish(event);

        // Simulate Kafka reporting a send failure via the callback
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(TOPIC, 0), 0, 0, 0L, 0, 0);
        callbackCaptor.getValue().onCompletion(metadata, new RuntimeException("Broker unavailable"));

        assertEquals(1, publisher.failedSendCount(),
                "failedSendCount should increment when send callback reports an error");
    }

    @SuppressWarnings("unchecked")
    @Test
    void successfulSendDoesNotIncrementFailedCount() {
        WikipediaEvent event = createEvent("Another Article", 0);

        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        when(producer.send(any(ProducerRecord.class), callbackCaptor.capture()))
                .thenReturn(mock(Future.class));

        publisher.publish(event);

        // Simulate successful send
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(TOPIC, 0), 0, 0, 0L, 0, 0);
        callbackCaptor.getValue().onCompletion(metadata, null);

        assertEquals(0, publisher.failedSendCount(),
                "failedSendCount should remain zero on successful sends");
    }

    /**
     * Helper to create a WikipediaEvent with the given title and namespace.
     */
    private WikipediaEvent createEvent(String title, int namespace) {
        return new WikipediaEvent(
                title,
                "enwiki",
                "TestUser",
                false,
                System.currentTimeMillis() / 1000,
                "edit",
                "Test edit",
                namespace,
                new WikipediaEvent.Revision(100, 101),
                new WikipediaEvent.Length(500, 520)
        );
    }
}
