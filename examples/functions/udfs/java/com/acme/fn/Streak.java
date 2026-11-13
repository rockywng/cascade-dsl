package com.acme.fn;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * A process table function: per partition key, the length of the current streak of successful
 * events. Keyed state, no SQL equivalent short of MATCH_RECOGNIZE.
 */
@FunctionHint(output = @DataTypeHint("ROW<streak INT, ok BOOLEAN>"))
public class Streak extends ProcessTableFunction<Row> {
    public static class State {
        public int streak = 0;
    }

    public void eval(@StateHint State state, @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        Boolean ok = input.getFieldAs("ok");
        state.streak = Boolean.TRUE.equals(ok) ? state.streak + 1 : 0;
        collect(Row.of(state.streak, ok));
    }
}
