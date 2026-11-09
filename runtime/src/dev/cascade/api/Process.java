package dev.cascade.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

/**
 * A process model: DataStream code inside a cascade pipeline, for what SQL can't express (keyed
 * state and timers, broadcast state, async I/O, custom windows and triggers, side outputs).
 *
 * <p>{@link #apply} gets one {@code DataStream<Row>} per declared input, in order, carrying the
 * inputs' event-time timestamps and watermarks (and keyed by {@code key_by} when declared). It
 * returns a stream of rows matching the declared {@code columns}; build it with
 * {@code .returns(ctx.outputType())}. Give every stateful operator {@code .uid(ctx.uid("..."))} so
 * its state survives upgrades. Rows of updating inputs carry their {@link org.apache.flink.types.RowKind}.
 */
public interface Process extends Serializable {
    DataStream<Row> apply(ProcessContext ctx, List<DataStream<Row>> inputs) throws Exception;
}
