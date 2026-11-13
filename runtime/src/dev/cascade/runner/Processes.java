package dev.cascade.runner;

import dev.cascade.api.Process;
import dev.cascade.api.ProcessContext;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs the {@code -- @@process {...}} directives cascade puts in a job script for process models:
 * turns the input views into DataStreams, calls the model's {@link Process} and registers its
 * output (and side outputs) as views for the SQL that follows.
 */
public final class Processes {
    public static final String DIRECTIVE = "-- @@process";
    private static final ObjectMapper JSON = new ObjectMapper();

    private Processes() {}

    public static boolean isDirective(String stmt) {
        return stmt.startsWith(DIRECTIVE);
    }

    @SuppressWarnings("unchecked")
    public static void apply(StreamTableEnvironment tEnv, String stmt) throws Exception {
        Map<String, Object> spec = JSON.readValue(stmt.substring(DIRECTIVE.length()).trim(), Map.class);
        String name = (String) spec.get("name");
        if ("python".equals(spec.get("language"))) {
            throw new IllegalStateException("process model " + name + " is Python: run this job with the Python "
                    + "runner (cascade_dsl/pyrunner.py; cascade deploy does this for pipelines with Python process models)");
        }
        String view = (String) spec.get("view");
        List<String> inputs = (List<String>) spec.get("inputs");
        List<String> keyBy = (List<String>) spec.getOrDefault("key_by", new ArrayList<>());
        Map<String, String> config = new LinkedHashMap<>();
        ((Map<String, Object>) spec.getOrDefault("config", new LinkedHashMap<>()))
                .forEach((k, v) -> config.put(k, String.valueOf(v)));

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String jar = (String) spec.get("jar");
        if (jar != null) {
            URL url = new File(jar).toURI().toURL();
            // the job's user classloader must see the jar too: ship it with the job
            List<String> jars = new ArrayList<>(tEnv.getConfig().getConfiguration()
                    .getOptional(PipelineOptions.JARS).orElse(new ArrayList<>()));
            if (!jars.contains(url.toString())) {
                jars.add(url.toString());
                tEnv.getConfig().getConfiguration().set(PipelineOptions.JARS, jars);
            }
            loader = new URLClassLoader(new URL[] {url}, loader);
        }
        Process process = (Process) Class.forName((String) spec.get("class"), true, loader)
                .getDeclaredConstructor().newInstance();

        List<DataStream<Row>> streams = new ArrayList<>();
        for (String input : inputs) {
            Table t = tEnv.from("`" + input + "`");
            DataStream<Row> ds = tEnv.toChangelogStream(t);
            if (!keyBy.isEmpty()) {
                List<String> names = t.getResolvedSchema().getColumnNames();
                int[] idx = new int[keyBy.size()];
                for (int i = 0; i < idx.length; i++) {
                    idx[i] = names.indexOf(keyBy.get(i));
                    if (idx[i] < 0) {
                        throw new IllegalArgumentException(
                                "process model " + name + ": input " + input + " has no column " + keyBy.get(i));
                    }
                }
                ds = ds.keyBy(new FieldsKey(idx), Types.STRING);
            }
            streams.add(ds);
        }

        DataType rowType = rowType((List<List<String>>) spec.get("columns"), loader);
        Map<String, Object> sides = (Map<String, Object>) spec.getOrDefault("side_outputs", new LinkedHashMap<>());
        Map<String, OutputTag<Row>> tags = new LinkedHashMap<>();
        Map<String, DataType> sideTypes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : sides.entrySet()) {
            Map<String, Object> side = (Map<String, Object>) e.getValue();
            DataType st = rowType((List<List<String>>) side.get("columns"), loader);
            sideTypes.put(e.getKey(), st);
            tags.put(e.getKey(), new OutputTag<Row>(name + "." + e.getKey(), ExternalTypeInfo.of(st)) {});
        }
        TypeInformation<Row> outType = ExternalTypeInfo.of(rowType);
        ProcessContext ctx = new ProcessContext() {
            public String name() { return name; }
            public Map<String, String> config() { return config; }
            public String uid(String operator) { return name + "." + operator; }
            public TypeInformation<Row> outputType() { return outType; }
            public OutputTag<Row> sideOutput(String side) {
                OutputTag<Row> tag = tags.get(side);
                if (tag == null) {
                    throw new IllegalArgumentException("process model " + name + " declares no side output " + side);
                }
                return tag;
            }
        };
        DataStream<Row> out = process.apply(ctx, streams);
        if (out == null) {
            throw new IllegalStateException("process model " + name + ": apply() returned null");
        }
        register(tEnv, view, out, rowType, spec);
        for (Map.Entry<String, OutputTag<Row>> e : tags.entrySet()) {
            if (!(out instanceof SingleOutputStreamOperator)) {
                throw new IllegalStateException("process model " + name + " declares side outputs, so apply() must "
                        + "return the operator that emits them (a SingleOutputStreamOperator)");
            }
            DataStream<Row> side = ((SingleOutputStreamOperator<Row>) out).getSideOutput(e.getValue());
            Map<String, Object> sideSpec = (Map<String, Object>) sides.get(e.getKey());
            register(tEnv, (String) sideSpec.get("view"), side, sideTypes.get(e.getKey()), sideSpec);
        }
    }

    @SuppressWarnings("unchecked")
    private static void register(StreamTableEnvironment tEnv, String view, DataStream<Row> stream, DataType rowType,
                                 Map<String, Object> spec) {
        Schema.Builder schema = Schema.newBuilder().fromRowDataType(rowType);
        String eventTime = (String) spec.get("event_time");
        if (eventTime != null) {
            schema.watermark(eventTime, "SOURCE_WATERMARK()");  // the stream's own watermarks
        }
        List<String> pk = (List<String>) spec.getOrDefault("primary_key", new ArrayList<>());
        if (!pk.isEmpty()) {
            schema.primaryKey(pk);
        }
        String changelog = (String) spec.getOrDefault("changelog", "append");
        ChangelogMode mode = "upsert".equals(changelog) ? ChangelogMode.upsert()
                : "retract".equals(changelog) ? ChangelogMode.all() : ChangelogMode.insertOnly();
        tEnv.createTemporaryView("`" + view + "`", tEnv.fromChangelogStream(stream, schema.build(), mode));
    }

    private static DataType rowType(List<List<String>> columns, ClassLoader loader) {
        List<DataTypes.Field> fields = new ArrayList<>();
        for (List<String> c : columns) {
            fields.add(DataTypes.FIELD(c.get(0),
                    TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(c.get(1), loader))));
        }
        return DataTypes.ROW(fields.toArray(new DataTypes.Field[0])).notNull();
    }

    /** Key = the key columns' values; a String so it needs no type information of its own. */
    static final class FieldsKey implements KeySelector<Row, String> {
        private final int[] idx;

        FieldsKey(int[] idx) {
            this.idx = idx;
        }

        @Override
        public String getKey(Row row) {
            StringBuilder sb = new StringBuilder();
            for (int i : idx) {
                Object v = row.getField(i);
                sb.append(v == null ? "\u0000" : v.toString()).append('\u0001');
            }
            return sb.toString();
        }
    }
}
