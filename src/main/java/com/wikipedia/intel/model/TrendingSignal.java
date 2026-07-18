package com.wikipedia.intel.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Signal indicating an article is trending — it received more edits than the
 * configured threshold within a single tumbling window.
 */
public record TrendingSignal(
    String title,
    int editCount,
    List<String> recentComments,
    int totalBytesChanged,
    long windowStart,
    long windowEnd
) implements Signal {

    /**
     * Constructor with all fields.
     */
    @JsonCreator
    public TrendingSignal(
            @JsonProperty("title") String title,
            @JsonProperty("editCount") int editCount,
            @JsonProperty("recentComments") List<String> recentComments,
            @JsonProperty("totalBytesChanged") int totalBytesChanged,
            @JsonProperty("windowStart") long windowStart,
            @JsonProperty("windowEnd") long windowEnd) {
        this.title = title;
        this.editCount = editCount;
        this.recentComments = recentComments != null ? recentComments : List.of();
        this.totalBytesChanged = totalBytesChanged;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String signalType() {
        return "TRENDING";
    }
}
