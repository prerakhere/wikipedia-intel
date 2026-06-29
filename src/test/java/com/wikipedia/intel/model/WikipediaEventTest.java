package com.wikipedia.intel.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WikipediaEvent record deserialization and helper methods.
 * Written TDD-style — these tests define the expected API before implementation.
 */
class WikipediaEventTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
    }

    @Test
    void deserializesRealSseJsonSample() throws Exception {
        String json = """
                {
                  "$schema": "/mediawiki/recentchange/1.0.0",
                  "meta": {
                    "uri": "https://en.wikipedia.org/wiki/Example",
                    "dt": "2024-01-01T00:00:00Z",
                    "domain": "en.wikipedia.org"
                  },
                  "id": 123,
                  "type": "edit",
                  "namespace": 0,
                  "title": "Example",
                  "comment": "minor fix",
                  "timestamp": 1704067200,
                  "user": "Editor1",
                  "bot": false,
                  "wiki": "enwiki",
                  "length": {"old": 1000, "new": 1050},
                  "revision": {"old": 100, "new": 101}
                }
                """;

        WikipediaEvent event = mapper.readValue(json, WikipediaEvent.class);

        assertEquals("Example", event.title());
        assertEquals("enwiki", event.wiki());
        assertEquals("Editor1", event.user());
        assertFalse(event.bot());
        assertEquals(1704067200L, event.timestamp());
        assertEquals("edit", event.type());
        assertEquals("minor fix", event.comment());
        assertEquals(0, event.namespace());

        assertNotNull(event.revision());
        assertEquals(100L, event.revision().old());
        assertEquals(101L, event.revision().current());

        assertNotNull(event.length());
        assertEquals(1000, event.length().old());
        assertEquals(1050, event.length().current());
    }

    @Test
    void isMainNamespaceReturnsTrueForNamespaceZero() throws Exception {
        String json = """
                {
                  "title": "Main Article",
                  "wiki": "enwiki",
                  "user": "User1",
                  "bot": false,
                  "timestamp": 1704067200,
                  "type": "edit",
                  "comment": "edit",
                  "namespace": 0,
                  "revision": {"old": 1, "new": 2},
                  "length": {"old": 100, "new": 200}
                }
                """;

        WikipediaEvent event = mapper.readValue(json, WikipediaEvent.class);

        assertTrue(event.isMainNamespace());
    }

    @Test
    void isMainNamespaceReturnsFalseForNonZeroNamespace() throws Exception {
        String json = """
                {
                  "title": "Talk:Main Article",
                  "wiki": "enwiki",
                  "user": "User1",
                  "bot": false,
                  "timestamp": 1704067200,
                  "type": "edit",
                  "comment": "discussion",
                  "namespace": 1,
                  "revision": {"old": 1, "new": 2},
                  "length": {"old": 100, "new": 200}
                }
                """;

        WikipediaEvent event = mapper.readValue(json, WikipediaEvent.class);

        assertFalse(event.isMainNamespace());
    }

    @Test
    void isMainNamespaceReturnsFalseForUserNamespace() throws Exception {
        String json = """
                {
                  "title": "User:SomeUser/Sandbox",
                  "wiki": "enwiki",
                  "user": "SomeUser",
                  "bot": false,
                  "timestamp": 1704067200,
                  "type": "edit",
                  "comment": "sandbox edit",
                  "namespace": 2,
                  "revision": {"old": 5, "new": 6},
                  "length": {"old": 50, "new": 75}
                }
                """;

        WikipediaEvent event = mapper.readValue(json, WikipediaEvent.class);

        assertFalse(event.isMainNamespace());
    }

    @Test
    void ignoresUnknownFieldsInJson() throws Exception {
        // Real SSE events contain many fields we don't map (meta, $schema, id, etc.)
        // @JsonIgnoreProperties(ignoreUnknown = true) should handle this gracefully
        String json = """
                {
                  "$schema": "/mediawiki/recentchange/1.0.0",
                  "meta": {
                    "uri": "https://en.wikipedia.org/wiki/Test",
                    "dt": "2024-06-15T12:30:00Z",
                    "domain": "en.wikipedia.org",
                    "stream": "mediawiki.recentchange",
                    "request_id": "abc-123"
                  },
                  "id": 99999,
                  "type": "edit",
                  "namespace": 0,
                  "title": "Test Article",
                  "comment": "testing",
                  "timestamp": 1718451000,
                  "user": "TestEditor",
                  "bot": true,
                  "wiki": "enwiki",
                  "length": {"old": 500, "new": 520},
                  "revision": {"old": 200, "new": 201},
                  "server_url": "https://en.wikipedia.org",
                  "server_name": "en.wikipedia.org",
                  "server_script_path": "/w",
                  "parsedcomment": "testing parsed",
                  "minor": true,
                  "patrolled": false,
                  "notify_url": "https://en.wikipedia.org/w/index.php?diff=201"
                }
                """;

        // Should not throw — unknown fields are silently ignored
        WikipediaEvent event = mapper.readValue(json, WikipediaEvent.class);

        assertEquals("Test Article", event.title());
        assertEquals("enwiki", event.wiki());
        assertEquals("TestEditor", event.user());
        assertTrue(event.bot());
        assertEquals(1718451000L, event.timestamp());
        assertEquals("edit", event.type());
        assertEquals("testing", event.comment());
        assertEquals(0, event.namespace());
    }
}
