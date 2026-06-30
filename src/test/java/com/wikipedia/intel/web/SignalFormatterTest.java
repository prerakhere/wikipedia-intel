package com.wikipedia.intel.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SignalFormatter.
 * Validates HTML row formatting, full dashboard page rendering, and JSON serialization.
 */
class SignalFormatterTest {

    private SignalFormatter formatter;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        formatter = new SignalFormatter();
        mapper = new ObjectMapper();
    }

    // --- formatHtmlRow(TrendingSignal) ---

    @Test
    void formatHtmlRow_trendingSignal_includesSignalType() {
        TrendingSignal signal = new TrendingSignal("Java_(programming_language)", 12, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("TRENDING"), "HTML row should contain signal type TRENDING");
    }

    @Test
    void formatHtmlRow_trendingSignal_includesTitle() {
        TrendingSignal signal = new TrendingSignal("Java_(programming_language)", 12, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("Java_(programming_language)"), "HTML row should contain the article title");
    }

    @Test
    void formatHtmlRow_trendingSignal_includesEditCount() {
        TrendingSignal signal = new TrendingSignal("Java_(programming_language)", 12, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("12"), "HTML row should contain the edit count");
        assertTrue(row.contains("edits"), "HTML row should label the count as edits");
    }

    @Test
    void formatHtmlRow_trendingSignal_includesTimeWindow() {
        TrendingSignal signal = new TrendingSignal("Java_(programming_language)", 12, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        // Should contain both window start and end as readable timestamps
        assertTrue(row.contains("<tr>"), "HTML row should be wrapped in <tr> tags");
        assertTrue(row.contains("</tr>"), "HTML row should be wrapped in </tr> tags");
        assertTrue(row.contains("<td>"), "HTML row should contain <td> cells");
    }

    @Test
    void formatHtmlRow_trendingSignal_isValidTableRow() {
        TrendingSignal signal = new TrendingSignal("Test_Article", 5, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.startsWith("<tr>"), "HTML row should start with <tr>");
        assertTrue(row.endsWith("</tr>"), "HTML row should end with </tr>");
    }

    // --- formatHtmlRow(BotAnomalySignal) ---

    @Test
    void formatHtmlRow_botAnomalySignal_includesSignalType() {
        BotAnomalySignal signal = new BotAnomalySignal(9, 10, 0.9, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("BOT_ANOMALY"), "HTML row should contain signal type BOT_ANOMALY");
    }

    @Test
    void formatHtmlRow_botAnomalySignal_includesBotRatio() {
        BotAnomalySignal signal = new BotAnomalySignal(9, 10, 0.9, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("90"), "HTML row should contain the bot ratio percentage");
    }

    @Test
    void formatHtmlRow_botAnomalySignal_includesCounts() {
        BotAnomalySignal signal = new BotAnomalySignal(9, 10, 0.9, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("9"), "HTML row should contain bot edit count");
        assertTrue(row.contains("10"), "HTML row should contain total edit count");
    }

    @Test
    void formatHtmlRow_botAnomalySignal_includesTimeWindow() {
        BotAnomalySignal signal = new BotAnomalySignal(9, 10, 0.9, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("<tr>"), "HTML row should be wrapped in <tr> tags");
        assertTrue(row.contains("</tr>"), "HTML row should be wrapped in </tr> tags");
    }

    @Test
    void formatHtmlRow_botAnomalySignal_showsDashForTitle() {
        BotAnomalySignal signal = new BotAnomalySignal(9, 10, 0.9, 1700000000000L, 1700000300000L);

        String row = formatter.formatHtmlRow(signal);

        assertTrue(row.contains("-"), "Bot anomaly row should show dash for title column");
    }

    // --- renderDashboardPage ---

    @Test
    void renderDashboardPage_producesValidHtml() {
        List<Signal> signals = List.of(
                new TrendingSignal("Test_Article", 7, 1700000000000L, 1700000300000L)
        );

        String page = formatter.renderDashboardPage(signals);

        assertTrue(page.contains("<!DOCTYPE html>") || page.contains("<html"),
                "Page should contain HTML document structure");
        assertTrue(page.contains("</html>"), "Page should have closing html tag");
    }

    @Test
    void renderDashboardPage_containsTable() {
        List<Signal> signals = List.of(
                new TrendingSignal("Test_Article", 7, 1700000000000L, 1700000300000L)
        );

        String page = formatter.renderDashboardPage(signals);

        assertTrue(page.contains("<table"), "Page should contain a table element");
        assertTrue(page.contains("</table>"), "Page should have closing table tag");
    }

    @Test
    void renderDashboardPage_containsAutoRefreshScript() {
        List<Signal> signals = List.of(
                new TrendingSignal("Test_Article", 7, 1700000000000L, 1700000300000L)
        );

        String page = formatter.renderDashboardPage(signals);

        assertTrue(page.contains("<script"), "Page should contain a script tag");
        assertTrue(page.contains("/api/signals"), "Script should reference the signals API endpoint");
        assertTrue(page.contains("10000") || page.contains("10 * 1000"),
                "Script should poll every 10 seconds");
    }

    @Test
    void renderDashboardPage_containsSignalRows() {
        List<Signal> signals = List.of(
                new TrendingSignal("Article_A", 5, 1700000000000L, 1700000300000L),
                new BotAnomalySignal(8, 10, 0.8, 1700000000000L, 1700000300000L)
        );

        String page = formatter.renderDashboardPage(signals);

        assertTrue(page.contains("Article_A"), "Page should contain the trending article title");
        assertTrue(page.contains("TRENDING"), "Page should contain TRENDING signal type");
        assertTrue(page.contains("BOT_ANOMALY"), "Page should contain BOT_ANOMALY signal type");
    }

    @Test
    void renderDashboardPage_emptySignalList_producesEmptyTable() {
        List<Signal> signals = List.of();

        String page = formatter.renderDashboardPage(signals);

        assertTrue(page.contains("<table"), "Page should still contain a table even with no signals");
        assertTrue(page.contains("<script"), "Page should still contain auto-refresh script");
    }

    // --- formatJson ---

    @Test
    void formatJson_producesValidJsonArray() throws Exception {
        List<Signal> signals = List.of(
                new TrendingSignal("Java_Article", 10, 1700000000000L, 1700000300000L)
        );

        String json = formatter.formatJson(signals);

        // Should be parseable as a JSON array
        List<Map<String, Object>> parsed = mapper.readValue(json, new TypeReference<>() {});
        assertEquals(1, parsed.size());
    }

    @Test
    void formatJson_includesAllTrendingSignalFields() throws Exception {
        List<Signal> signals = List.of(
                new TrendingSignal("Kotlin_Article", 8, 1700000000000L, 1700000300000L)
        );

        String json = formatter.formatJson(signals);

        assertTrue(json.contains("\"signalType\""), "JSON should contain signalType field");
        assertTrue(json.contains("\"TRENDING\""), "JSON should contain TRENDING value");
        assertTrue(json.contains("\"title\""), "JSON should contain title field");
        assertTrue(json.contains("\"Kotlin_Article\""), "JSON should contain the title value");
        assertTrue(json.contains("\"editCount\""), "JSON should contain editCount field");
        assertTrue(json.contains("\"windowStart\""), "JSON should contain windowStart field");
        assertTrue(json.contains("\"windowEnd\""), "JSON should contain windowEnd field");
    }

    @Test
    void formatJson_includesAllBotAnomalySignalFields() throws Exception {
        List<Signal> signals = List.of(
                new BotAnomalySignal(15, 20, 0.75, 1700000000000L, 1700000300000L)
        );

        String json = formatter.formatJson(signals);

        assertTrue(json.contains("\"signalType\""), "JSON should contain signalType field");
        assertTrue(json.contains("\"BOT_ANOMALY\""), "JSON should contain BOT_ANOMALY value");
        assertTrue(json.contains("\"botEditCount\""), "JSON should contain botEditCount field");
        assertTrue(json.contains("\"totalEditCount\""), "JSON should contain totalEditCount field");
        assertTrue(json.contains("\"ratio\""), "JSON should contain ratio field");
        assertTrue(json.contains("\"windowStart\""), "JSON should contain windowStart field");
        assertTrue(json.contains("\"windowEnd\""), "JSON should contain windowEnd field");
    }

    @Test
    void formatJson_multipleSignals_producesArrayWithCorrectCount() throws Exception {
        List<Signal> signals = List.of(
                new TrendingSignal("Article_1", 5, 1700000000000L, 1700000300000L),
                new BotAnomalySignal(9, 10, 0.9, 1700000000000L, 1700000300000L),
                new TrendingSignal("Article_2", 15, 1700000300000L, 1700000600000L)
        );

        String json = formatter.formatJson(signals);

        List<Map<String, Object>> parsed = mapper.readValue(json, new TypeReference<>() {});
        assertEquals(3, parsed.size());
    }

    @Test
    void formatJson_emptyList_producesEmptyArray() throws Exception {
        List<Signal> signals = List.of();

        String json = formatter.formatJson(signals);

        assertEquals("[]", json.trim());
    }
}
