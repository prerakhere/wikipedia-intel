package com.wikipedia.intel.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed interface for all signal types emitted by the Signal_Detector.
 * Uses Jackson polymorphic type handling to serialize/deserialize via a "signalType" discriminator.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "signalType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = TrendingSignal.class, name = "TRENDING"),
    @JsonSubTypes.Type(value = BotAnomalySignal.class, name = "BOT_ANOMALY")
})
public sealed interface Signal permits TrendingSignal, BotAnomalySignal {
    String signalType();
    long windowStart();
    long windowEnd();
}
