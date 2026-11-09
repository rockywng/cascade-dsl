package dev.cascade.cdc;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Last row per primary key, plus the slot generation that last saw the key.
 *
 * <p>Every change is turned into an exact one against that row: an insert or update of an existing
 * key becomes -U old / +U new (or nothing if unchanged), a delete retracts the stored row. That makes
 * a re-snapshot (every current row again, as inserts) exact. When the stream phase of a
 * re-snapshot begins (a marker), keys the new generation never saw were deleted during the gap:
 * they are retracted. Markers repeat after failovers; the sweep is idempotent.
 */
final class Reconcile extends KeyedBroadcastProcessFunction<String, RowData, RowData, RowData> {
    private final RowType type;
    private final String generation;
    private transient ValueStateDescriptor<RowData> desc;
    private transient ValueState<RowData> last;
    private transient TypeSerializer<RowData> stored;
    private transient RowData.FieldGetter[] getters;

    Reconcile(RowType type, String generation) {
        this.type = type;
        this.generation = generation;
    }

    @Override
    public void open(OpenContext ctx) {
        List<RowType.RowField> fields = new ArrayList<>(type.getFields());
        fields.add(new RowType.RowField("_cascade_generation", new VarCharType(VarCharType.MAX_LENGTH)));
        InternalTypeInfo<RowData> info = InternalTypeInfo.of(new RowType(fields));
        stored = info.toRowSerializer();
        desc = new ValueStateDescriptor<>("last-row", info);
        last = getRuntimeContext().getState(desc);
        getters = new RowData.FieldGetter[type.getFieldCount()];
        for (int i = 0; i < getters.length; i++) {
            LogicalType t = type.getTypeAt(i);
            getters[i] = RowData.createFieldGetter(t, i);
        }
    }

    @Override
    public void processElement(RowData row, ReadOnlyContext ctx, Collector<RowData> out) throws Exception {
        RowKind kind = row.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return; // the old row comes from state
        }
        RowData prev = last.value();
        if (kind == RowKind.DELETE) {
            if (prev != null) {
                out.collect(emit(prev, RowKind.DELETE));
                last.clear();
            }
            return;
        }
        if (prev != null && same(prev, row)) {
            if (!generation.equals(prev.getString(getters.length).toString())) {
                last.update(store(row)); // seen by this generation
            }
            return;
        }
        if (prev != null) {
            out.collect(emit(prev, RowKind.UPDATE_BEFORE));
            out.collect(emit(row, RowKind.UPDATE_AFTER));
        } else {
            out.collect(emit(row, RowKind.INSERT));
        }
        last.update(store(row));
    }

    @Override
    public void processBroadcastElement(RowData marker, Context ctx, Collector<RowData> out) throws Exception {
        // the re-snapshot is complete: what this generation never saw was deleted during the gap
        ctx.applyToKeyedState(desc, (key, state) -> {
            RowData v = state.value();
            if (v != null && !generation.equals(v.getString(getters.length).toString())) {
                out.collect(emit(v, RowKind.DELETE));
                state.clear();
            }
        });
    }

    private RowData store(RowData row) {
        GenericRowData g = new GenericRowData(getters.length + 1);
        for (int i = 0; i < getters.length; i++) {
            g.setField(i, getters[i].getFieldOrNull(row));
        }
        g.setField(getters.length, StringData.fromString(generation));
        return stored.copy(g); // deep copy: upstream may reuse the row's buffers
    }

    private RowData emit(RowData row, RowKind kind) {
        GenericRowData g = new GenericRowData(kind, getters.length);
        for (int i = 0; i < getters.length; i++) {
            g.setField(i, getters[i].getFieldOrNull(row));
        }
        return g;
    }

    private boolean same(RowData a, RowData b) {
        for (RowData.FieldGetter g : getters) {
            Object x = g.getFieldOrNull(a);
            Object y = g.getFieldOrNull(b);
            if (x instanceof byte[] && y instanceof byte[]) {
                if (!Arrays.equals((byte[]) x, (byte[]) y)) {
                    return false;
                }
            } else if (!Objects.equals(x, y)) {
                return false;
            }
        }
        return true;
    }
}
