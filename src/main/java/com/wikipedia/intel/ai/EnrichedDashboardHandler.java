package com.wikipedia.intel.ai;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * HTTP handler for the AI-enriched dashboard.
 * Serves an HTML page with enriched signals and a JSON API endpoint.
 */
public class EnrichedDashboardHandler implements HttpHandler {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(IST);
    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(IST);

    private final CopyOnWriteArrayList<EnrichedSignal> signals;
    private final ObjectMapper mapper = new ObjectMapper();

    public EnrichedDashboardHandler(CopyOnWriteArrayList<EnrichedSignal> signals) {
        this.signals = signals;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String response;
        String contentType;

        if ("/api/enriched".equals(path)) {
            contentType = "application/json; charset=UTF-8";
            response = mapper.writeValueAsString(signals);
        } else {
            contentType = "text/html; charset=UTF-8";
            response = renderPage();
        }

        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String renderPage() {
        StringBuilder rows = new StringBuilder();
        // Show sorted by confidence descending
        List<EnrichedSignal> sorted = signals.stream()
                .sorted((a, b) -> Double.compare(b.confidence(), a.confidence()))
                .toList();

        for (EnrichedSignal s : sorted) {
            String attentionClass = switch (s.attentionLevel()) {
                case "high" -> "attention-high";
                case "medium" -> "attention-medium";
                default -> "attention-low";
            };
            String wikiUrl = "https://en.wikipedia.org/wiki/" + s.title().replace(' ', '_');
            String time = formatTime(s.windowStart(), s.windowEnd());
            rows.append(String.format(
                    "            <tr class=\"%s\"><td><a href=\"%s\" target=\"_blank\">%s</a></td>"
                    + "<td>%s</td><td>%s</td><td>%d edits</td><td>%.0f%%</td><td>%s</td><td>%s</td></tr>\n",
                    attentionClass, wikiUrl, s.title(),
                    s.eventType(), s.summary(), s.editCount(),
                    s.confidence() * 100, s.attentionLevel(), time));
        }

        return """
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="UTF-8">
                    <title>Wikipedia Intel - AI Enriched Dashboard</title>
                    <style>
                        body { font-family: sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
                        h1 { color: #e94560; }
                        table { border-collapse: collapse; width: 100%%; background: #16213e; }
                        th, td { border: 1px solid #0f3460; padding: 10px; text-align: left; }
                        th { background: #0f3460; color: #e94560; }
                        a { color: #4fc3f7; text-decoration: none; }
                        a:hover { text-decoration: underline; }
                        .attention-high { background: rgba(233, 69, 96, 0.15); }
                        .attention-medium { background: rgba(255, 193, 7, 0.1); }
                        .attention-low { background: transparent; }
                        .status { color: #888; margin-bottom: 15px; }
                    </style>
                </head>
                <body>
                    <h1>🧠 Wikipedia Intel - AI Enriched</h1>
                    <p class="status">Signals: <span id="count">%d</span> | Auto-refresh: 10s</p>
                    <table>
                        <thead>
                            <tr>
                                <th>Article</th>
                                <th>Event Type</th>
                                <th>Summary</th>
                                <th>Edits</th>
                                <th>Confidence</th>
                                <th>Attention</th>
                                <th>Time</th>
                            </tr>
                        </thead>
                        <tbody id="signal-body">
                %s
                        </tbody>
                    </table>
                    <script>
                        setInterval(function() {
                            fetch('/api/enriched')
                                .then(function(r) { return r.json(); })
                                .then(function(signals) {
                                    signals.sort(function(a, b) { return b.confidence - a.confidence; });
                                    document.getElementById('count').textContent = signals.length;
                                    var body = document.getElementById('signal-body');
                                    body.innerHTML = '';
                                    signals.forEach(function(s) {
                                        var row = document.createElement('tr');
                                        var cls = s.attentionLevel === 'high' ? 'attention-high' : s.attentionLevel === 'medium' ? 'attention-medium' : 'attention-low';
                                        row.className = cls;
                                        var url = 'https://en.wikipedia.org/wiki/' + encodeURIComponent((s.title || '').replace(/ /g, '_'));
                                        var time = new Date(s.windowStart).toLocaleTimeString('en-GB', {timeZone: 'Asia/Kolkata', hour12: false}) + ' - ' + new Date(s.windowEnd).toLocaleTimeString('en-GB', {timeZone: 'Asia/Kolkata', hour12: false});
                                        row.innerHTML = '<td><a href="' + url + '" target="_blank">' + (s.title || '-') + '</a></td>'
                                            + '<td>' + (s.eventType || '-') + '</td>'
                                            + '<td>' + (s.summary || '-') + '</td>'
                                            + '<td>' + s.editCount + ' edits</td>'
                                            + '<td>' + Math.round(s.confidence * 100) + '%%</td>'
                                            + '<td>' + (s.attentionLevel || '-') + '</td>'
                                            + '<td>' + time + '</td>';
                                        body.appendChild(row);
                                    });
                                });
                        }, 10000);
                    </script>
                </body>
                </html>
                """.formatted(sorted.size(), rows.toString());
    }

    private String formatTime(long startMs, long endMs) {
        return TIME_FORMAT.format(Instant.ofEpochMilli(startMs)) + " - " + TIME_FORMAT.format(Instant.ofEpochMilli(endMs));
    }
}
