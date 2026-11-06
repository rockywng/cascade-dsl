package dev.cascade.runtime;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Backs {@code on_violation: fail} expectations: CASCADE_ASSERT(condition, description).
 *
 * <p>Returns TRUE when the condition holds and throws otherwise, which fails the task and lets
 * the job's restart strategy / alerting take over. Non-deterministic so the planner never
 * constant-folds or reorders it away.
 */
public class AssertFunction extends ScalarFunction {

    private transient String operator;

    @Override
    public void open(FunctionContext context) {
        operator = context.getJobParameter("pipeline.name", "pipeline");
    }

    public Boolean eval(Boolean condition, String description) {
        if (condition == null || !condition) {
            throw new ExpectationViolation(operator + ": expectation failed: " + description);
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
