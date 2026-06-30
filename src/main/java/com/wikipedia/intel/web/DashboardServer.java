package com.wikipedia.intel.web;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Lightweight embedded HTTP server using JDK's built-in com.sun.net.httpserver.
 * Serves the signal dashboard HTML page and JSON API.
 */
public class DashboardServer {

    private static final Logger logger = LoggerFactory.getLogger(DashboardServer.class);

    private final HttpServer server;
    private final int port;

    /**
     * Creates and configures the HTTP server on the given port.
     *
     * @param port    TCP port to bind (default: 8080)
     * @param handler the request handler for serving dashboard content
     * @throws IOException if the port cannot be bound
     */
    public DashboardServer(int port, SignalHandler handler) throws IOException {
        this.port = port;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", handler);
            server.createContext("/api/signals", handler);
        } catch (IOException e) {
            logger.error("Failed to bind dashboard server to port {}", port, e);
            throw e;
        }
    }

    /**
     * Starts the HTTP server. Non-blocking — uses an internal executor.
     */
    public void start() {
        server.setExecutor(null); // default executor
        server.start();
        logger.info("Dashboard server started on port {}", port);
    }

    /**
     * Stops the HTTP server gracefully with a brief drain period.
     */
    public void stop() {
        server.stop(1); // 1 second drain
        logger.info("Dashboard server stopped");
    }
}
