package dev.cascade.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/** What a {@link Process} knows about its model. */
public interface ProcessContext {
    /** The model's name. */
    String name();

    /** The model's {@code config} block. */
    Map<String, String> config();

    /** A stable operator uid, "{@code <model>.<operator>}": state is matched by it on restore. */
    String uid(String operator);

    /** Type of the declared output rows (use with {@code .returns(...)}). */
    TypeInformation<Row> outputType();

    /** The declared side output {@code name}: emit to it with {@code ctx.output(tag, row)}. */
    OutputTag<Row> sideOutput(String name);
}
