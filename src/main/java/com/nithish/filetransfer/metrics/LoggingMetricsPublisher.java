package com.nithish.filetransfer.metrics;

import com.nithish.filetransfer.logging.AppLog;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * The default publisher -- used whenever CLOUDWATCH_ENABLED is not
 * "true", and also the fallback MetricsPublisherFactory returns if
 * CloudWatch setup fails for any reason. Local development and testing
 * must never depend on AWS being reachable; this is how that's
 * guaranteed at the type level, not just by convention.
 */
public final class LoggingMetricsPublisher implements MetricsPublisher {

    @Override
    public void incrementCounter(String metricName, long delta, Map<String, String> dimensions) {
        AppLog.info("metric_counter", "name", metricName, "delta", String.valueOf(delta), "dimensions", formatDims(dimensions));
    }

    @Override
    public void recordValue(String metricName, double value, Map<String, String> dimensions) {
        AppLog.info("metric_value", "name", metricName, "value", String.valueOf(value), "dimensions", formatDims(dimensions));
    }

    private static String formatDims(Map<String, String> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) return "{}";
        return dimensions.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(",", "{", "}"));
    }
}
