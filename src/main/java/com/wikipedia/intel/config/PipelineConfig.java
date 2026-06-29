package com.wikipedia.intel.config;

/**
 * Centralized configuration for all pipeline components.
 * Loads from system properties > environment variables > defaults.
 *
 * <p>Property naming convention: {@code pipeline.bootstrap.servers}
 * <p>Environment variable convention: {@code PIPELINE_BOOTSTRAP_SERVERS}
 */
public record PipelineConfig(
    // Kafka
    String bootstrapServers,
    String inputTopic,
    String signalsTopic,

    // SSE
    String sseUrl,
    long reconnectInitialMs,
    long reconnectMaxMs,

    // Trending detection
    long trendingWindowMinutes,
    int trendingThreshold,

    // Bot anomaly detection
    long botWindowMinutes,
    double botRatioThreshold,
    int botMinimumVolume,

    // DynamoDB
    String dynamoTableName,
    String dynamoRegion,
    int dynamoMaxRetries,

    // Web Dashboard
    int dashboardPort
) {

    /**
     * Loads configuration using system properties, then environment variables, then defaults.
     * Priority: system property > environment variable > default value.
     */
    public static PipelineConfig load() {
        return new PipelineConfig(
            resolve("pipeline.bootstrap.servers", "PIPELINE_BOOTSTRAP_SERVERS", "localhost:9092"),
            resolve("pipeline.input.topic", "PIPELINE_INPUT_TOPIC", "wikipedia.recentchanges"),
            resolve("pipeline.signals.topic", "PIPELINE_SIGNALS_TOPIC", "wikipedia.signals"),
            resolve("pipeline.sse.url", "PIPELINE_SSE_URL",
                    "https://stream.wikimedia.org/v2/stream/recentchange"),
            Long.parseLong(resolve("pipeline.reconnect.initial.ms", "PIPELINE_RECONNECT_INITIAL_MS", "1000")),
            Long.parseLong(resolve("pipeline.reconnect.max.ms", "PIPELINE_RECONNECT_MAX_MS", "30000")),
            Long.parseLong(resolve("pipeline.trending.window.minutes", "PIPELINE_TRENDING_WINDOW_MINUTES", "5")),
            Integer.parseInt(resolve("pipeline.trending.threshold", "PIPELINE_TRENDING_THRESHOLD", "5")),
            Long.parseLong(resolve("pipeline.bot.window.minutes", "PIPELINE_BOT_WINDOW_MINUTES", "5")),
            Double.parseDouble(resolve("pipeline.bot.ratio.threshold", "PIPELINE_BOT_RATIO_THRESHOLD", "0.8")),
            Integer.parseInt(resolve("pipeline.bot.minimum.volume", "PIPELINE_BOT_MINIMUM_VOLUME", "10")),
            resolve("pipeline.dynamo.table.name", "PIPELINE_DYNAMO_TABLE_NAME", "wikipedia-signals"),
            resolve("pipeline.dynamo.region", "PIPELINE_DYNAMO_REGION", "ap-south-1"),
            Integer.parseInt(resolve("pipeline.dynamo.max.retries", "PIPELINE_DYNAMO_MAX_RETRIES", "3")),
            Integer.parseInt(resolve("pipeline.dashboard.port", "PIPELINE_DASHBOARD_PORT", "8080"))
        );
    }

    /**
     * Resolves a configuration value with priority: system property > environment variable > default.
     */
    private static String resolve(String systemProperty, String envVariable, String defaultValue) {
        String value = System.getProperty(systemProperty);
        if (value != null && !value.isBlank()) {
            return value;
        }
        value = System.getenv(envVariable);
        if (value != null && !value.isBlank()) {
            return value;
        }
        return defaultValue;
    }
}
