package dev.cascade.cdc;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** The primary key as a String (stable, needs no type information of its own). */
final class KeyOf implements KeySelector<RowData, String> {
    private final RowData.FieldGetter[] getters;

    KeyOf(RowType type, int[] keys) {
        getters = new RowData.FieldGetter[keys.length];
        for (int i = 0; i < keys.length; i++) {
            getters[i] = RowData.createFieldGetter(type.getTypeAt(keys[i]), keys[i]);
        }
    }

    @Override
    public String getKey(RowData row) {
        StringBuilder sb = new StringBuilder();
        for (RowData.FieldGetter g : getters) {
            Object v = g.getFieldOrNull(row);
            sb.append(v == null ? "\u0000" : v.toString()).append('\u0001');
        }
        return sb.toString();
    }
}
