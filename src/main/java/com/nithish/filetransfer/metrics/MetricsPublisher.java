package com.nithish.filetransfer.metrics;

import java.util.Map;

/**
 * Every metric this application publishes goes through this interface --
 * TransferProcessor, TransferServer, and FileIntegrityMonitor call these
 * methods on their real counters at the exact point those counters
 * already change. Nothing here invents a metric that doesn't correspond
 * to real application behavior.
 *
 * Implementations must NEVER throw and must NEVER block the caller on a
 * slow/failed network call for longer than necessary -- a metrics
 * failure must not become a transfer failure (see CloudWatchMetricsPublisher's
 * javadoc and MetricsPublisherFactory for how that's enforced).
 */
public interface MetricsPublisher {

    /** Low-cardinality dimensions only (e.g. Transport=TCP/UDP, Environment=local/cloud) -- never transfer_id, filename, or client IP. */
    void incrementCounter(String metricName, long delta, Map<String, String> dimensions);

    /** For a measured value that isn't a running count -- durations, active-connection snapshots. */
    void recordValue(String metricName, double value, Map<String, String> dimensions);

    default void incrementCounter(String metricName) {
        incrementCounter(metricName, 1, Map.of());
    }

    default void incrementCounter(String metricName, long delta) {
        incrementCounter(metricName, delta, Map.of());
    }

    default void recordValue(String metricName, double value) {
        recordValue(metricName, value, Map.of());
    }

    /** Release any held resources (e.g. an HTTP client). Local/logging implementations can no-op. */
    default void close() {}
}
