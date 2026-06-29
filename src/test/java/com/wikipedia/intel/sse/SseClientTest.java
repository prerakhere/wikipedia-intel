package com.wikipedia.intel.sse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.WikipediaEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SseClient.
 * Tests SSE stream parsing via the package-private processStream method,
 * verifying correct data extraction, malformed JSON handling, and callback invocation.
 */
class SseClientTest {

    private ObjectMapper mapper;
    private SseClient client;
    private List<WikipediaEvent> receivedEvents;
    private Consumer<WikipediaEvent> callback;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        client = new SseClient("https://stream.wikimedia.org/v2/stream/recentchange", mapper);
        receivedEvents = new ArrayList<>();
        callback = receivedEvents::add;
    }

    @Test
    void validDataLineIsParsedAndCallbackInvoked() {
        String json = """
                {"title":"Java (programming language)","wiki":"enwiki","user":"TestUser","bot":false,"timestamp":1700000000,"type":"edit","comment":"fix typo","namespace":0,"revision":{"old":100,"new":101},"length":{"old":500,"new":520}}""";
        String sseStream = "data: " + json + "\n\n";

        InputStream stream = toInputStream(sseStream);
        client.processStream(stream, callback);

        assertEquals(1, receivedEvents.size(), "Callback should be invoked exactly once");
        WikipediaEvent event = receivedEvents.get(0);
        assertEquals("Java (programming language)", event.title());
        assertEquals("enwiki", event.wiki());
        assertEquals("TestUser", event.user());
        assertFalse(event.bot());
        assertEquals(0, event.namespace());
        assertEquals(101, event.revision().current());
        assertEquals(520, event.length().current());
    }

    @Test
    void malformedJsonLineIsSkippedWithoutCallback() {
        String sseStream = "data: {not valid json at all!}\n\n";

        InputStream stream = toInputStream(sseStream);
        client.processStream(stream, callback);

        assertTrue(receivedEvents.isEmpty(),
                "Callback should not be invoked for malformed JSON");
    }

    @Test
    void nonDataLinesAreIgnored() {
        // SSE streams can contain comments (: prefix), event type lines, id lines, and empty lines
        String sseStream = """
                : this is a comment
                event: message
                id: 12345
                
                """;

        InputStream stream = toInputStream(sseStream);
        client.processStream(stream, callback);

        assertTrue(receivedEvents.isEmpty(),
                "Non-data lines should not trigger callbacks");
    }

    @Test
    void multipleEventsAllTriggerCallbacks() {
        String json1 = """
                {"title":"Article One","wiki":"enwiki","user":"User1","bot":false,"timestamp":1700000001,"type":"edit","comment":"edit 1","namespace":0,"revision":{"old":1,"new":2},"length":{"old":100,"new":110}}""";
        String json2 = """
                {"title":"Article Two","wiki":"enwiki","user":"User2","bot":true,"timestamp":1700000002,"type":"edit","comment":"edit 2","namespace":1,"revision":{"old":3,"new":4},"length":{"old":200,"new":210}}""";
        String json3 = """
                {"title":"Article Three","wiki":"dewiki","user":"User3","bot":false,"timestamp":1700000003,"type":"new","comment":"new page","namespace":0,"revision":{"old":0,"new":5},"length":{"old":0,"new":300}}""";

        String sseStream = "data: " + json1 + "\n\n"
                + "data: " + json2 + "\n\n"
                + "data: " + json3 + "\n\n";

        InputStream stream = toInputStream(sseStream);
        client.processStream(stream, callback);

        assertEquals(3, receivedEvents.size(), "All three events should trigger callbacks");
        assertEquals("Article One", receivedEvents.get(0).title());
        assertEquals("Article Two", receivedEvents.get(1).title());
        assertEquals("Article Three", receivedEvents.get(2).title());
    }

    @Test
    void dataLineWithExtraFieldsIsParsedSuccessfully() {
        // WikipediaEvent uses @JsonIgnoreProperties(ignoreUnknown = true)
        String json = """
                {"title":"Test Page","wiki":"enwiki","user":"Bot1","bot":true,"timestamp":1700000000,"type":"edit","comment":"automated","namespace":0,"revision":{"old":10,"new":11},"length":{"old":50,"new":55},"unknown_field":"should be ignored","another":123}""";
        String sseStream = "data: " + json + "\n\n";

        InputStream stream = toInputStream(sseStream);
        client.processStream(stream, callback);

        assertEquals(1, receivedEvents.size());
        assertEquals("Test Page", receivedEvents.get(0).title());
        assertTrue(receivedEvents.get(0).bot());
    }

    @Test
    void malformedLineAmidValidEventsDoesNotStopProcessing() {
        String validJson1 = """
                {"title":"Before","wiki":"enwiki","user":"U1","bot":false,"timestamp":1700000001,"type":"edit","comment":"c1","namespace":0,"revision":{"old":1,"new":2},"length":{"old":10,"new":20}}""";
        String validJson2 = """
                {"title":"After","wiki":"enwiki","user":"U2","bot":false,"timestamp":1700000002,"type":"edit","comment":"c2","namespace":0,"revision":{"old":3,"new":4},"length":{"old":30,"new":40}}""";

        String sseStream = "data: " + validJson1 + "\n\n"
                + "data: {broken json\n\n"
                + "data: " + validJson2 + "\n\n";

        InputStream stream = toInputStream(sseStream);
        client.processStream(stream, callback);

        assertEquals(2, receivedEvents.size(),
                "Malformed event should be skipped but processing should continue");
        assertEquals("Before", receivedEvents.get(0).title());
        assertEquals("After", receivedEvents.get(1).title());
    }

    @Test
    void dataLinePrefixWithoutSpaceIsHandled() {
        // SSE spec allows "data:" with or without the space after the colon
        String json = """
                {"title":"No Space","wiki":"enwiki","user":"U","bot":false,"timestamp":1700000000,"type":"edit","comment":"c","namespace":0,"revision":{"old":1,"new":2},"length":{"old":10,"new":20}}""";
        String sseStream = "data:" + json + "\n\n";

        InputStream stream = toInputStream(sseStream);
        client.processStream(stream, callback);

        assertEquals(1, receivedEvents.size());
        assertEquals("No Space", receivedEvents.get(0).title());
    }

    private InputStream toInputStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
