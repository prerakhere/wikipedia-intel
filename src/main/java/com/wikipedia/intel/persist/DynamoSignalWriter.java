package com.wikipedia.intel.persist;

import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * Writes Signal instances to DynamoDB with retry logic.
 * Retries up to 3 times with exponential backoff (100ms, 200ms, 400ms).
 * After all retries are exhausted, logs an ERROR and gives up without throwing.
 */
public class DynamoSignalWriter {

    private static final Logger log = LoggerFactory.getLogger(DynamoSignalWriter.class);
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 100;

    private final DynamoDbClient dynamoClient;
    private final DynamoKeyStrategy keyStrategy;
    private final String tableName;

    public DynamoSignalWriter(DynamoDbClient dynamoClient, DynamoKeyStrategy keyStrategy, String tableName) {
        this.dynamoClient = dynamoClient;
        this.keyStrategy = keyStrategy;
        this.tableName = tableName;
    }

    /**
     * Writes a signal to DynamoDB. Retries up to 3 times with exponential backoff.
     * Does not throw on failure — logs ERROR after retries are exhausted.
     */
    public void write(Signal signal) {
        PutItemRequest request = buildPutItemRequest(signal);

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                dynamoClient.putItem(request);
                return; // Success
            } catch (Exception e) {
                if (attempt < MAX_RETRIES) {
                    long backoffMs = INITIAL_BACKOFF_MS * (1L << (attempt - 1));
                    log.warn("DynamoDB write attempt {} failed, retrying in {}ms: {}",
                            attempt, backoffMs, e.getMessage());
                    sleep(backoffMs);
                } else {
                    log.error("DynamoDB write failed after {} retries for signal type={}, windowEnd={}: {}",
                            MAX_RETRIES, signal.signalType(), signal.windowEnd(), e.getMessage());
                }
            }
        }
    }

    private PutItemRequest buildPutItemRequest(Signal signal) {
        Map<String, AttributeValue> item = new HashMap<>();

        // Keys
        item.put("pk", AttributeValue.builder().s(keyStrategy.partitionKey(signal)).build());
        item.put("sk", AttributeValue.builder().s(keyStrategy.sortKey(signal)).build());

        // Common fields
        item.put("signalType", AttributeValue.builder().s(signal.signalType()).build());
        item.put("windowStart", AttributeValue.builder().n(String.valueOf(signal.windowStart())).build());
        item.put("windowEnd", AttributeValue.builder().n(String.valueOf(signal.windowEnd())).build());

        // Type-specific fields
        switch (signal) {
            case TrendingSignal trending -> {
                item.put("title", AttributeValue.builder().s(trending.title()).build());
                item.put("editCount", AttributeValue.builder().n(String.valueOf(trending.editCount())).build());
            }
            case BotAnomalySignal botAnomaly -> {
                item.put("botEditCount", AttributeValue.builder().n(String.valueOf(botAnomaly.botEditCount())).build());
                item.put("totalEditCount", AttributeValue.builder().n(String.valueOf(botAnomaly.totalEditCount())).build());
                item.put("ratio", AttributeValue.builder().n(String.valueOf(botAnomaly.ratio())).build());
            }
        }

        return PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
