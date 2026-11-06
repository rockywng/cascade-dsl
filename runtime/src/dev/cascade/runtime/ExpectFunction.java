package dev.cascade.runtime;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Backs {@code on_violation: drop | quarantine} expectations:
 * CASCADE_EXPECT(condition, model, expectation, action) used as a filter predicate.
 *
 * <p>Returns whether the row passes, and counts checked/violating rows so dropped data is visible
 * on dashboards instead of disappearing silently.
 */
public class ExpectFunction extends ScalarFunction {

    private transient ExpectationMetrics metrics;

    @Override
    public void open(FunctionContext context) {
        metrics = new ExpectationMetrics(context.getMetricGroup());
    }

    public Boolean eval(Boolean condition, String model, String expectation, String action) {
        boolean ok = condition != null && condition;
        metrics.record(model, expectation, action, ok);
        return ok;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
