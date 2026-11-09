package com.acme.process;

import dev.cascade.api.Process;
import dev.cascade.api.ProcessContext;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Click sessions with an inactivity gap: keyed state plus event-time timers, and late clicks (behind
 * the watermark) to a side output -- none of which SQL windows can do with a per-user gap and a
 * session that closes exactly `gap` after its last click.
 */
public class Sessionize implements Process {
    @Override
    @SuppressWarnings("unchecked")
    public DataStream<Row> apply(ProcessContext ctx, List<DataStream<Row>> inputs) {
        long gap = Duration.parse(ctx.config().getOrDefault("gap", "PT30M")).toMillis();
        KeyedStream<Row, String> clicks = (KeyedStream<Row, String>) inputs.get(0); // key_by: [user_id]
        return clicks.process(new Sessions(gap, ctx.sideOutput("late")))
                .returns(ctx.outputType())
                .uid(ctx.uid("sessionize"))
                .name("sessionize");
    }

    static final class Sessions extends KeyedProcessFunction<String, Row, Row> {
        private final long gap;
        private final OutputTag<Row> late;
        private transient ValueState<Long> start;
        private transient ValueState<Long> end;
        private transient ValueState<Long> count;
        private transient ValueState<String> user;

        Sessions(long gap, OutputTag<Row> late) {
            this.gap = gap;
            this.late = late;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext context) {
            start = getRuntimeContext().getState(new ValueStateDescriptor<>("start", Types.LONG));
            end = getRuntimeContext().getState(new ValueStateDescriptor<>("end", Types.LONG));
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
            user = getRuntimeContext().getState(new ValueStateDescriptor<>("user", Types.STRING));
        }

        @Override
        public void processElement(Row click, Context ctx, Collector<Row> out) throws Exception {
            long ts = ctx.timestamp();
            if (ts <= ctx.timerService().currentWatermark()) {
                ctx.output(late, Row.of(click.getField("user_id"), click.getField("url"), time(ts)));
                return;
            }
            Long s = start.value();
            Long e = end.value();
            if (e != null) {
                ctx.timerService().deleteEventTimeTimer(e + gap);
                if (ts > e + gap) {
                    // the gap passed before the watermark did: close the old session now
                    emit(out, e + gap);
                    s = null;
                    e = null;
                }
            }
            start.update(s == null ? ts : Math.min(s, ts));
            end.update(e == null ? ts : Math.max(e, ts));
            count.update(s == null ? 1 : count.value() + 1);
            user.update((String) click.getField("user_id"));
            ctx.timerService().registerEventTimeTimer(end.value() + gap);
        }

        @Override
        public void onTimer(long t, OnTimerContext ctx, Collector<Row> out) throws Exception {
            // the row's timestamp is t (= session_closed): on time for the watermark that fired it
            emit(out, t);
        }

        private void emit(Collector<Row> out, long closed) throws Exception {
            out.collect(Row.of(user.value(), time(start.value()), time(end.value()), count.value(), time(closed)));
            start.clear();
            end.clear();
            count.clear();
            user.clear();
        }

        private static LocalDateTime time(long millis) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
        }
    }
}
