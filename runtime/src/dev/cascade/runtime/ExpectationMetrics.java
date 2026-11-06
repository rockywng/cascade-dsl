package dev.cascade.runtime;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-expectation counters, exposed with labels so reporters (Prometheus, OTel) produce e.g.
 * {@code cascade_expectation_violations{model="clean_orders",expectation="positive_amount",action="drop"}}.
 */
final class ExpectationMetrics {

    private final MetricGroup root;
    private final Map<String, Counter[]> counters = new HashMap<>();

    ExpectationMetrics(MetricGroup root) {
        this.root = root;
    }

    void record(String model, String expectation, String action, boolean ok) {
        Counter[] c =
                counters.computeIfAbsent(
                        model + "\u0000" + expectation,
                        k -> {
                            MetricGroup g =
                                    root.addGroup("cascade_model", model)
                                            .addGroup("expectation", expectation)
                                            .addGroup("action", action);
                            return new Counter[] {
                                g.counter("cascade_expectation_checked"),
                                g.counter("cascade_expectation_violations")
                            };
                        });
        c[0].inc();
        if (!ok) {
            c[1].inc();
        }
    }
}
