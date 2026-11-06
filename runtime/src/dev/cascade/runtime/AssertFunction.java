package dev.cascade.runtime;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Backs {@code on_violation: fail} expectations:
 * CASCADE_ASSERT(condition, model, expectation, check).
 *
 * <p>Returns TRUE when the condition holds; otherwise counts the violation and throws, which fails
 * the task so the restart strategy and alerting take over. Non-deterministic so the planner never
 * constant-folds or reorders it away.
 */
public class AssertFunction extends ScalarFunction {

    private transient ExpectationMetrics metrics;
    private transient String pipeline;

    @Override
    public void open(FunctionContext context) {
        metrics = new ExpectationMetrics(context.getMetricGroup());
        pipeline = context.getJobParameter("pipeline.name", "pipeline");
    }

    public Boolean eval(Boolean condition, String model, String expectation, String check) {
        boolean ok = condition != null && condition;
        metrics.record(model, expectation, "fail", ok);
        if (!ok) {
            throw new ExpectationViolation(
                    pipeline + ": expectation " + model + "." + expectation + " failed: " + check);
        }
        return Boolean.TRUE;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    /** Distinct type so the platform can classify these failures as data-quality incidents. */
    public static class ExpectationViolation extends RuntimeException {
        public ExpectationViolation(String message) {
            super(message);
        }
    }
}
