package dev.cascade.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Postgres CDC with slot-loss healing.
 *
 * <pre>
 *   delegate CDC source, state tagged with the slot generation (TaggedSource)
 *     -> markers split off -> keyBy(primary key) -> Reconcile (last row per key) -> table
 * </pre>
 *
 * If the slot the saved position belongs to is gone, the runner moves the source to a new slot
 * generation ({@code slot.name}). The tagged source then ignores its saved position and re-snapshots;
 * Reconcile turns the snapshot into exact changes against what downstream already has and, once the
 * stream phase begins (the snapshot is complete), deletes the keys that vanished during the gap.
 */
public final class HealingCdcSource implements ScanTableSource {
    static final OutputTag<RowData> MARKERS = new OutputTag<RowData>("cascade-snapshot-complete") {};
    static final MapStateDescriptor<String, String> BROADCAST =
            new MapStateDescriptor<>("cascade-reconcile-markers", Types.STRING, Types.STRING);

    private final DynamicTableSource inner;
    private final RowType rowType;
    private final int[] keys;
    private final String slot;
    private final String name;

    HealingCdcSource(DynamicTableSource inner, RowType rowType, int[] keys, String slot, String name) {
        this.inner = inner;
        this.rowType = rowType;
        this.keys = keys;
        this.slot = slot;
        this.name = name;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        ScanRuntimeProvider p = ((ScanTableSource) inner).getScanRuntimeProvider(context);
        if (!(p instanceof SourceProvider)) {
            throw new IllegalStateException("cascade-healing-cdc needs the incremental (FLIP-27) CDC source "
                    + "(scan.incremental.snapshot.enabled = true)");
        }
        Source<RowData, ?, ?> source = (Source<RowData, ?, ?>) ((SourceProvider) p).createSource();
        RowType type = rowType;
        int[] k = keys;
        String generation = slot;
        String n = name;
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext ctx, StreamExecutionEnvironment env) {
                InternalTypeInfo<RowData> info = InternalTypeInfo.of(type);
                DataStream<RowData> cdc = env.fromSource(
                                new TaggedSource(source, generation, type.getFieldCount()),
                                WatermarkStrategy.noWatermarks(), n + " (cdc)", info)
                        .uid(ctx.generateUid("cdc").orElse(n + ".cdc"));
                SingleOutputStreamOperator<RowData> rows = cdc.process(new SplitMarkers(k), info)
                        .uid(ctx.generateUid("markers").orElse(n + ".markers")).name(n + " (markers)");
                BroadcastStream<RowData> markers = rows.getSideOutput(MARKERS).broadcast(BROADCAST);
                return rows.keyBy(new KeyOf(type, k), Types.STRING).connect(markers)
                        .process(new Reconcile(type, generation), info)
                        .uid(ctx.generateUid("reconcile").orElse(n + ".reconcile")).name(n + " (reconcile)");
            }

            @Override
            public boolean isBounded() {
                return false;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new HealingCdcSource(inner.copy(), rowType, keys, slot, name);
    }

    @Override
    public String asSummaryString() {
        return "cascade-healing-cdc(" + inner.asSummaryString() + ")";
    }

    /** A marker: an all-null UPDATE_BEFORE (a real change never has a null primary key). */
    static boolean isMarker(RowData row, int[] keys) {
        if (row.getRowKind() != org.apache.flink.types.RowKind.UPDATE_BEFORE) {
            return false;
        }
        for (int k : keys) {
            if (!row.isNullAt(k)) {
                return false;
            }
        }
        return true;
    }

    static final class SplitMarkers extends ProcessFunction<RowData, RowData> {
        private final int[] keys;

        SplitMarkers(int[] keys) {
            this.keys = keys;
        }

        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out) {
            if (isMarker(row, keys)) {
                ctx.output(MARKERS, row);
            } else {
                out.collect(row);
            }
        }
    }
}
