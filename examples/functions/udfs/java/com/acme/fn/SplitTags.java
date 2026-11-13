package com.acme.fn;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/** One row per comma-separated tag, with its position. */
@FunctionHint(output = @DataTypeHint("ROW<tag STRING, pos INT>"))
public class SplitTags extends TableFunction<Row> {
    public void eval(String tags) {
        if (tags == null) {
            return;
        }
        String[] parts = tags.split(",");
        for (int i = 0; i < parts.length; i++) {
            collect(Row.of(parts[i].trim(), i));
        }
    }
}
