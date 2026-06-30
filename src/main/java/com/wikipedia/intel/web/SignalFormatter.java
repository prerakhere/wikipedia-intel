package com.wikipedia.intel.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.intel.model.BotAnomalySignal;
import com.wikipedia.intel.model.Signal;
import com.wikipedia.intel.model.TrendingSignal;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Formats Signal instances for dashboard display (HTML and JSON).
 */
public class SignalFormatter {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private final ObjectMapper mapper;

    public SignalFormatter() {
        this.mapper = new ObjectMapper();
    }

    /**
     * Formats a signal as an HTML table row.
     * Includes: signal type, metrics, article title (if trending), time window.
     */
    public String formatHtmlRow(Signal signal) {
        return switch (signal) {
            case TrendingSignal t -> formatTrendingRow(t);
            case BotAnomalySignal b -> formatBotAnomalyRow(b);
        };
    }

    /**
     * Renders the full HTML dashboard page with the given signals embedded in a table.
     * Includes auto-refresh JavaScript that polls /api/signals every 10 seconds.
     */
    public String renderDashboardPage(List<Signal> signals) {
        StringBuilder rows = new StringBuilder();
        for (Signal signal : signals) {
            rows.append("            ").append(formatHtmlRow(signal)).append("\n");
        }

        return "<!DOCTYPE html>\n"
                + "<html>\n"
                + "<head>\n"
                + "    <meta charset=\"UTF-8\">\n"
                + "    <title>Wikipedia Intel Dashboard</title>\n"
                + "    <style>\n"
                + "        body { font-family: sans-serif; margin: 20px; background: #f5f5f5; }\n"
                + "        h1 { color: #333; }\n"
                + "        table { border-collapse: collapse; width: 100%; background: white; }\n"
                + "        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n"
                + "        th { background: #4CAF50; color: white; }\n"
                + "        tr:nth-child(even) { background: #f2f2f2; }\n"
                + "    </style>\n"
                + "</head>\n"
                + "<body>\n"
                + "    <h1>Wikipedia Intel - Signal Dashboard</h1>\n"
                + "    <table>\n"
                + "        <thead>\n"
                + "            <tr>\n"
                + "                <th>Type</th>\n"
                + "                <th>Title</th>\n"
                + "                <th>Metrics</th>\n"
                + "                <th>Time Window</th>\n"
                + "            </tr>\n"
                + "        </thead>\n"
                + "        <tbody id=\"signal-body\">\n"
                + rows
                + "        </tbody>\n"
                + "    </table>\n"
                + "    <script>\n"
                + "        setInterval(function() {\n"
                + "            fetch('/api/signals')\n"
                + "                .then(function(response) { return response.json(); })\n"
                + "                .then(function(signals) {\n"
                + "                    var body = document.getElementById('signal-body');\n"
                + "                    body.innerHTML = '';\n"
                + "                    signals.forEach(function(s) {\n"
                + "                        var row = document.createElement('tr');\n"
                + "                        var type = s.signalType || '';\n"
                + "                        var title = s.title || '-';\n"
                + "                        var metrics = '';\n"
                + "                        if (type === 'TRENDING') {\n"
                + "                            metrics = s.editCount + ' edits';\n"
                + "                        } else if (type === 'BOT_ANOMALY') {\n"
                + "                            metrics = s.botEditCount + '/' + s.totalEditCount + ' (' + Math.round(s.ratio * 100) + '%)';\n"
                + "                        }\n"
                + "                        var window = new Date(s.windowStart).toISOString() + ' - ' + new Date(s.windowEnd).toISOString();\n"
                + "                        row.innerHTML = '<td>' + type + '</td><td>' + title + '</td><td>' + metrics + '</td><td>' + window + '</td>';\n"
                + "                        body.appendChild(row);\n"
                + "                    });\n"
                + "                });\n"
                + "        }, 10000);\n"
                + "    </script>\n"
                + "</body>\n"
                + "</html>\n";
    }

    /**
     * Serializes a list of signals to a JSON array string using Jackson.
     * Uses a type-aware writer to preserve polymorphic type information.
     */
    public String formatJson(List<Signal> signals) {
        try {
            return mapper.writerFor(mapper.getTypeFactory()
                    .constructCollectionType(List.class, Signal.class))
                    .writeValueAsString(signals);
        } catch (JsonProcessingException e) {
            return "[]";
        }
    }

    private String formatTrendingRow(TrendingSignal signal) {
        String windowStart = formatTimestamp(signal.windowStart());
        String windowEnd = formatTimestamp(signal.windowEnd());
        return "<tr><td>TRENDING</td><td>%s</td><td>%d edits</td><td>%s - %s</td></tr>".formatted(
                signal.title(), signal.editCount(), windowStart, windowEnd);
    }

    private String formatBotAnomalyRow(BotAnomalySignal signal) {
        String windowStart = formatTimestamp(signal.windowStart());
        String windowEnd = formatTimestamp(signal.windowEnd());
        int ratioPercent = (int) Math.round(signal.ratio() * 100);
        return "<tr><td>BOT_ANOMALY</td><td>-</td><td>%d/%d (%d%%)</td><td>%s - %s</td></tr>".formatted(
                signal.botEditCount(), signal.totalEditCount(), ratioPercent, windowStart, windowEnd);
    }

    private String formatTimestamp(long epochMillis) {
        return TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(epochMillis));
    }
}
