package com.wikipedia.intel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Deserialized Wikipedia EventStreams SSE event.
 * Immutable record mapping the JSON fields we care about.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikipediaEvent(
    String title,
    String wiki,
    String user,
    boolean bot,
    long timestamp,
    String type,
    String comment,
    @JsonProperty("namespace") int namespace,
    Revision revision,
    Length length
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Revision(long old, @JsonProperty("new") long current) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Length(int old, @JsonProperty("new") int current) {}

    /** Returns true if this event belongs to the main article namespace. */
    public boolean isMainNamespace() {
        return namespace == 0;
    }
}
