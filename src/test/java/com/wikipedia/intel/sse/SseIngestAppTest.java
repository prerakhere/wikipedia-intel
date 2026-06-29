package com.wikipedia.intel.sse;

import com.wikipedia.intel.model.WikipediaEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SseIngestApp integration.
 * Tests the reconnection loop, wiring of SseClient to EventPublisher, and graceful shutdown.
 *
 * TDD: SseIngestApp does NOT exist yet — these tests define the expected behavior.
 */
class SseIngestAppTest {

    private SseClient sseClient;
    private EventPublisher publisher;
    private SseReconnector reconnector;

    @BeforeEach
    void setUp() {
        sseClient = mock(SseClient.class);
        publisher = mock(EventPublisher.class);
        reconnector = mock(SseReconnector.class);
    }

    @Test
    void connectCallbackIsWiredToEventPublisher() throws InterruptedException {
        // Arrange: when connect() is called, capture the callback and invoke it with a test event
        WikipediaEvent testEvent = createEvent("Wiring Test", 0);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<WikipediaEvent> callback = invocation.getArgument(0);
            callback.accept(testEvent);
            return null;
        }).doAnswer(invocation -> {
            // Second call: just return immediately to let reconnection kick in
            return null;
        }).when(sseClient).connect(any());

        // Make reconnector return a delay, then stop the app on second attempt
        when(reconnector.computeDelay(0)).thenReturn(10L);

        SseIngestApp app = new SseIngestApp(sseClient, publisher, reconnector);

        // Run in a separate thread since run() blocks
        Thread appThread = new Thread(app::run);
        appThread.start();

        // Give it time to process
        Thread.sleep(100);
        app.stop();
        appThread.join(1000);

        // Assert: the event was forwarded to EventPublisher.publish()
        verify(publisher).publish(testEvent);
    }

    @Test
    void reconnectionLoopInvokesSseReconnectorOnDisconnect() throws InterruptedException {
        // Arrange: SseClient.connect() returns immediately (simulates disconnect)
        AtomicInteger connectCount = new AtomicInteger(0);
        CountDownLatch reconnectLatch = new CountDownLatch(3);

        doAnswer(invocation -> {
            connectCount.incrementAndGet();
            reconnectLatch.countDown();
            return null;
        }).when(sseClient).connect(any());

        // Return very short delays so the test runs fast
        when(reconnector.computeDelay(anyInt())).thenReturn(5L);

        SseIngestApp app = new SseIngestApp(sseClient, publisher, reconnector);

        Thread appThread = new Thread(app::run);
        appThread.start();

        // Wait for at least 3 reconnection attempts
        boolean reached = reconnectLatch.await(2, TimeUnit.SECONDS);
        app.stop();
        appThread.join(1000);

        // Assert: reconnector was consulted for backoff delays
        assertTrue(reached, "Expected at least 3 reconnection attempts");
        assertTrue(connectCount.get() >= 3, "SseClient.connect() should be called multiple times");
        verify(reconnector, atLeast(2)).computeDelay(anyInt());
    }

    @Test
    void stopCausesReconnectionLoopToExit() throws InterruptedException {
        // Arrange: connect() returns immediately each time (simulates repeated disconnects)
        doNothing().when(sseClient).connect(any());
        when(reconnector.computeDelay(anyInt())).thenReturn(10L);

        SseIngestApp app = new SseIngestApp(sseClient, publisher, reconnector);

        Thread appThread = new Thread(app::run);
        appThread.start();

        // Let it run a few iterations
        Thread.sleep(50);

        // Act: stop the app
        app.stop();

        // Assert: thread exits within a reasonable time
        appThread.join(2000);
        assertFalse(appThread.isAlive(), "App thread should exit after stop() is called");
    }

    @Test
    void reconnectionAttemptCounterIncrementsOnEachDisconnect() throws InterruptedException {
        // Arrange: track which attempt numbers are passed to computeDelay
        AtomicInteger connectCalls = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        doAnswer(invocation -> {
            connectCalls.incrementAndGet();
            latch.countDown();
            return null;
        }).when(sseClient).connect(any());

        when(reconnector.computeDelay(anyInt())).thenReturn(5L);

        SseIngestApp app = new SseIngestApp(sseClient, publisher, reconnector);

        Thread appThread = new Thread(app::run);
        appThread.start();

        latch.await(2, TimeUnit.SECONDS);
        app.stop();
        appThread.join(1000);

        // Assert: computeDelay called with increasing attempt numbers (0, 1, 2, ...)
        verify(reconnector).computeDelay(0);
        verify(reconnector).computeDelay(1);
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
