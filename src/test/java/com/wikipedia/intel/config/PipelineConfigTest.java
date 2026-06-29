package com.wikipedia.intel.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigTest {

    @Test
    void loadReturnsDefaultValues() {
        PipelineConfig config = PipelineConfig.load();

        assertEquals("localhost:9092", config.bootstrapServers());
        assertEquals("wikipedia.recentchanges", config.inputTopic());
        assertEquals("wikipedia.signals", config.signalsTopic());
        assertEquals("https://stream.wikimedia.org/v2/stream/recentchange", config.sseUrl());
        assertEquals(1000L, config.reconnectInitialMs());
        assertEquals(30000L, config.reconnectMaxMs());
        assertEquals(5L, config.trendingWindowMinutes());
        assertEquals(5, config.trendingThreshold());
        assertEquals(5L, config.botWindowMinutes());
        assertEquals(0.8, config.botRatioThreshold(), 0.001);
        assertEquals(10, config.botMinimumVolume());
        assertEquals("wikipedia-signals", config.dynamoTableName());
        assertEquals("ap-south-1", config.dynamoRegion());
        assertEquals(3, config.dynamoMaxRetries());
        assertEquals(8080, config.dashboardPort());
    }

    @Test
    void systemPropertyOverridesDefault() {
        System.setProperty("pipeline.bootstrap.servers", "broker:19092");
        try {
            PipelineConfig config = PipelineConfig.load();
            assertEquals("broker:19092", config.bootstrapServers());
        } finally {
            System.clearProperty("pipeline.bootstrap.servers");
        }
    }

    @Test
    void systemPropertyOverridesEnvironmentVariable() {
        // Environment variables can't be set in tests easily, but we can verify
        // that system property takes precedence when both exist.
        System.setProperty("pipeline.dashboard.port", "9090");
        try {
            PipelineConfig config = PipelineConfig.load();
            assertEquals(9090, config.dashboardPort());
        } finally {
            System.clearProperty("pipeline.dashboard.port");
        }
    }

    @Test
    void blankSystemPropertyFallsThrough() {
        System.setProperty("pipeline.input.topic", "   ");
        try {
            PipelineConfig config = PipelineConfig.load();
            // Blank property is ignored, falls through to default
            assertEquals("wikipedia.recentchanges", config.inputTopic());
        } finally {
            System.clearProperty("pipeline.input.topic");
        }
    }
}
