package com.nithish.filetransfer.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/** Records every call for test assertions -- a real test double, not a mock framework. */
public final class RecordingMetricsPublisher implements MetricsPublisher {

    public final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    public final CopyOnWriteArrayList<double[]> recordedValues = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<String> recordedValueNames = new CopyOnWriteArrayList<>();
    public volatile boolean closed = false;

    @Override
    public void incrementCounter(String metricName, long delta, Map<String, String> dimensions) {
        counters.computeIfAbsent(metricName, k -> new AtomicLong()).addAndGet(delta);
    }

    @Override
    public void recordValue(String metricName, double value, Map<String, String> dimensions) {
        recordedValueNames.add(metricName);
        recordedValues.add(new double[]{value});
    }

    public long counterValue(String metricName) {
        AtomicLong c = counters.get(metricName);
        return c == null ? 0 : c.get();
    }

    @Override
    public void close() {
        closed = true;
    }
}
