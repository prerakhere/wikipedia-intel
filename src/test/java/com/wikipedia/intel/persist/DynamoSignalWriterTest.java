package com.wikipedia.intel.persist;

import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DynamoSignalWriter.
 * Tests successful writes, retry logic, and error logging after retries exhausted.
 */
class DynamoSignalWriterTest {

    private static final String TABLE_NAME = "wikipedia-signals";

    private DynamoDbClient dynamoClient;
    private DynamoKeyStrategy keyStrategy;
    private DynamoSignalWriter writer;

    @BeforeEach
    void setUp() {
        dynamoClient = mock(DynamoDbClient.class);
        keyStrategy = new DynamoKeyStrategy();
        writer = new DynamoSignalWriter(dynamoClient, keyStrategy, TABLE_NAME);
    }

    @Test
    void successfulWriteCallsPutItemWithCorrectKeyAndValue() {
        TrendingSignal signal = new TrendingSignal("Java_(programming_language)", 10, 1000L, 2000L);

        when(dynamoClient.putItem(any(PutItemRequest.class)))
                .thenReturn(PutItemResponse.builder().build());

        writer.write(signal);

        ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoClient).putItem(captor.capture());

        PutItemRequest request = captor.getValue();
        assertEquals(TABLE_NAME, request.tableName(), "Table name should match configured value");

        Map<String, AttributeValue> item = request.item();
        assertEquals(signal.signalType(), item.get("pk").s(),
                "Partition key should equal signal type");
        assertEquals(String.valueOf(signal.windowEnd()), item.get("sk").s(),
                "Sort key should equal window end timestamp as string");
    }

    @Test
    void retryOnFirstFailureThenSucceedDoesNotLogError() {
        TrendingSignal signal = new TrendingSignal("Retry_Article", 5, 3000L, 4000L);

        // First call fails, second succeeds
        when(dynamoClient.putItem(any(PutItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("Transient error").build())
                .thenReturn(PutItemResponse.builder().build());

        writer.write(signal);

        // putItem should be called exactly 2 times (1 failure + 1 success)
        verify(dynamoClient, times(2)).putItem(any(PutItemRequest.class));
    }

    @Test
    void threeFailuresExhaustsRetriesAndDoesNotThrow() {
        TrendingSignal signal = new TrendingSignal("Failing_Article", 7, 5000L, 6000L);

        // All 3 attempts fail
        when(dynamoClient.putItem(any(PutItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("Persistent error").build());

        // Should not throw — errors are logged, not propagated
        assertDoesNotThrow(() -> writer.write(signal),
                "write() should not throw even after all retries are exhausted");

        // putItem should be called exactly 3 times
        verify(dynamoClient, times(3)).putItem(any(PutItemRequest.class));
    }
}
