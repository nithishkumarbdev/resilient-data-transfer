package com.nithish.filetransfer.metrics;

import com.nithish.filetransfer.logging.AppLog;

/**
 * CLOUDWATCH_ENABLED=true is a REQUEST to use CloudWatch, not a
 * guarantee. CloudWatchMetricsPublisher is instantiated via reflection
 * rather than a direct `new`, deliberately: this class has zero
 * compile-time dependency on the AWS SDK, so a build that doesn't
 * include the (optional) AWS SDK dependency still compiles and runs
 * correctly in local mode -- it just can't construct the CloudWatch
 * publisher, and falls back.
 *
 * This isn't a workaround for a missing dependency; it's the actual
 * resilience property Phase 5 requires: if CloudWatch integration is
 * unavailable or misconfigured for any reason (missing SDK on the
 * classpath, bad credentials, no network route to AWS), the
 * application must keep working in local/logging mode rather than
 * fail to start. That exact path is what runs by default in this
 * development environment, since the AWS SDK isn't on its classpath --
 * which makes it a real, continuously-exercised test of the fallback,
 * not a hypothetical one.
 */
public final class MetricsPublisherFactory {

    private static final String CLOUDWATCH_PUBLISHER_CLASS = "com.nithish.filetransfer.metrics.CloudWatchMetricsPublisher";

    private MetricsPublisherFactory() {}

    public static MetricsPublisher create() {
        boolean enabled = "true".equalsIgnoreCase(System.getenv("CLOUDWATCH_ENABLED"));
        if (!enabled) {
            AppLog.info("metrics_publisher_selected", "mode", "local");
            return new LoggingMetricsPublisher();
        }

        String namespace = System.getenv().getOrDefault("CLOUDWATCH_NAMESPACE", "ProtobufTransferSystem");
        String environment = System.getenv().getOrDefault("CLOUDWATCH_ENVIRONMENT", "cloud");

        try {
            Class<?> publisherClass = Class.forName(CLOUDWATCH_PUBLISHER_CLASS);
            MetricsPublisher publisher = (MetricsPublisher) publisherClass
                    .getDeclaredConstructor(String.class, String.class)
                    .newInstance(namespace, environment);
            AppLog.info("metrics_publisher_selected", "mode", "cloudwatch", "namespace", namespace);
            return publisher;
        } catch (Throwable t) {
            // Any failure here -- SDK not on the classpath, AWS auth
            // failure, bad region config -- must not stop the server
            // from starting. Fall back to local logging and keep going.
            AppLog.warn("metrics_publisher_fallback", "reason", String.valueOf(t));
            return new LoggingMetricsPublisher();
        }
    }
}
