package com.nithish.filetransfer.metrics;

import com.nithish.filetransfer.logging.AppLog;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ============================================================================
 * NOT COMPILED OR TESTED IN THIS DEVELOPMENT ENVIRONMENT.
 *
 * This sandbox has no network path to AWS (amazonaws.com is blocked by
 * network policy) and the AWS SDK for Java is not available locally
 * (not via the package manager, and Maven Central is also unreachable
 * here). This file was written directly against the AWS SDK v2
 * CloudWatch client API from documented knowledge, but it has NOT been
 * compiled, NOT been run, and NOT been verified against a real
 * CloudWatch endpoint in this session.
 *
 * Before relying on this in a real deployment: run `mvn compile` with
 * the AWS SDK dependency present (see pom.xml) to confirm it actually
 * compiles, then run a real transfer with CLOUDWATCH_ENABLED=true
 * against a real IAM role and confirm metrics actually appear in
 * CloudWatch (see README's "What was NOT verified" section).
 * ============================================================================
 *
 * Uses the default credentials provider chain, which on an EC2 instance
 * with an attached instance profile picks up temporary credentials from
 * the instance metadata service automatically -- no access keys are
 * read from configuration, source, or environment variables here.
 *
 * Every public method catches Throwable and logs rather than
 * propagating: a CloudWatch outage, throttling response, or IAM
 * permission error must never surface as a transfer failure (see
 * MetricsPublisherFactory's javadoc for why this matters).
 */
public final class CloudWatchMetricsPublisher implements MetricsPublisher {

    private final CloudWatchClient client;
    private final String namespace;
    private final String environment;

    /** Constructor signature must match what MetricsPublisherFactory looks up via reflection. */
    public CloudWatchMetricsPublisher(String namespace, String environment) {
        this.namespace = namespace;
        this.environment = environment;
        // Region comes from the standard AWS SDK resolution chain (AWS_REGION
        // env var, ~/.aws/config, or EC2 instance metadata) -- not hardcoded,
        // so the same code works in whatever region the instance is deployed to.
        this.client = CloudWatchClient.builder().build();
    }

    @Override
    public void incrementCounter(String metricName, long delta, Map<String, String> dimensions) {
        publish(metricName, (double) delta, StandardUnit.COUNT, dimensions);
    }

    @Override
    public void recordValue(String metricName, double value, Map<String, String> dimensions) {
        publish(metricName, value, StandardUnit.NONE, dimensions);
    }

    private void publish(String metricName, double value, StandardUnit unit, Map<String, String> dimensions) {
        try {
            List<Dimension> dims = new ArrayList<>();
            dims.add(Dimension.builder().name("Environment").value(environment).build());
            if (dimensions != null) {
                for (Map.Entry<String, String> entry : dimensions.entrySet()) {
                    dims.add(Dimension.builder().name(entry.getKey()).value(entry.getValue()).build());
                }
            }

            MetricDatum datum = MetricDatum.builder()
                    .metricName(metricName)
                    .unit(unit)
                    .value(value)
                    .timestamp(Instant.now())
                    .dimensions(dims)
                    .build();

            PutMetricDataRequest request = PutMetricDataRequest.builder()
                    .namespace(namespace)
                    .metricData(datum)
                    .build();

            client.putMetricData(request);

        } catch (Throwable t) {
            // Network blip, throttling, missing permission, whatever it is --
            // this is observability failing, not the application. Log and move on.
            AppLog.warn("cloudwatch_publish_failed", "metric", metricName, "reason", String.valueOf(t));
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Throwable t) {
            AppLog.warn("cloudwatch_client_close_failed", "reason", String.valueOf(t));
        }
    }
}
