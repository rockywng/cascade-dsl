package dev.cascade.tools;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test-only connector "cascade-fixture" used by `cascade test`.
 *
 * <p>Source: reads JSON lines written by the Python test runner, in order, with one subtask:
 * <pre>
 *   {"kind": "+I", "values": [...]}     a row (kinds +I, -U, +U, -D), values in physical column order
 *   {"watermark": 1767261600000}        emit this watermark now (epoch millis)
 * </pre>
 * Tables declare {@code WATERMARK FOR ts AS SOURCE_WATERMARK()}, so these watermarks are the only
 * ones: late data in tests is deterministic. Also a lookup source (final state of the rows).
 *
 * <p>Sink: appends every change it receives as {"kind", "values"} JSON lines to a file, with the
 * changelog mode of the real sink ('mode' = append | upsert).
 */
public final class FixtureFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    static final ConfigOption<String> PATH = ConfigOptions.key("path").stringType().noDefaultValue();
    static final ConfigOption<String> CHANGELOG =
            ConfigOptions.key("changelog").stringType().defaultValue("insert-only");
    static final ConfigOption<String> MODE = ConfigOptions.key("mode").stringType().defaultValue("append");

    @Override
    public String factoryIdentifier() {
        return "cascade-fixture";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(PATH);
        return s;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(CHANGELOG);
        s.add(MODE);
        return s;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        return new Source(
                helper.getOptions().get(PATH),
                "all".equals(helper.getOptions().get(CHANGELOG)),
                context.getPhysicalRowDataType());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        return new Sink(
                helper.getOptions().get(PATH),
                "upsert".equals(helper.getOptions().get(MODE)),
                context.getPhysicalRowDataType());
    }

    // --- source -----------------------------------------------------------------------------

    static final class Source implements ScanTableSource, LookupTableSource, SupportsSourceWatermark {
        private final String path;
        private final boolean changelog;
        private final DataType type;

        Source(String path, boolean changelog, DataType type) {
            this.path = path;
            this.changelog = changelog;
            this.type = type;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return changelog ? ChangelogMode.all() : ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
            DynamicTableSource.DataStructureConverter converter = ctx.createDataStructureConverter(type);
            RowType rowType = (RowType) type.getLogicalType();
            return new DataStreamScanProvider() {
                @Override
                public DataStream<RowData> produceDataStream(
                        ProviderContext providerContext, StreamExecutionEnvironment env) {
                    List<String> lines;
                    try {
                        lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
                    } catch (IOException e) {
                        throw new RuntimeException("cannot read fixture " + path, e);
                    }
                    lines.removeIf(l -> l.trim().isEmpty());
                    if (lines.isEmpty()) {
                        lines.add("{\"skip\": true}"); // fromData needs at least one element
                    }
                    return env.fromData(lines)
                            .setParallelism(1)
                            .assignTimestampsAndWatermarks(
                                    WatermarkStrategy.<String>forGenerator(c -> new FileWatermarks()))
                            .setParallelism(1)
                            .flatMap(new ToRowData(converter, rowType))
                            .returns(InternalTypeInfo.of(rowType))
                            .setParallelism(1);
                }

                @Override
                public boolean isBounded() {
                    return true;
                }
            };
        }

        @Override
        public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext ctx) {
            return LookupFunctionProvider.of(
                    new Lookup(path, ctx.createDataStructureConverter(type), (RowType) type.getLogicalType(),
                            ctx.getKeys()));
        }

        @Override
        public void applySourceWatermark() {
            // watermarks come from the fixture file
        }

        @Override
        public DynamicTableSource copy() {
            return new Source(path, changelog, type);
        }

        @Override
        public String asSummaryString() {
            return "cascade-fixture";
        }
    }

    public static final class FileWatermarks implements WatermarkGenerator<String> {
        @Override
        public void onEvent(String line, long ts, WatermarkOutput out) {
            if (line.startsWith("{\"watermark\"")) {
                String v = line.substring(line.indexOf(':') + 1, line.lastIndexOf('}')).trim();
                out.emitWatermark(new Watermark(Long.parseLong(v)));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput out) {}
    }

    public static final class ToRowData extends RichFlatMapFunction<String, RowData> {
        private final DynamicTableSource.DataStructureConverter converter;
        private final RowType rowType;
        private transient ObjectMapper mapper;

        ToRowData(DynamicTableSource.DataStructureConverter converter, RowType rowType) {
            this.converter = converter;
            this.rowType = rowType;
        }

        @Override
        public void open(OpenContext ctx) {
            converter.open(RuntimeConverter.Context.create(getRuntimeContext().getUserCodeClassLoader()));
            mapper = new ObjectMapper();
        }

        @Override
        public void flatMap(String line, org.apache.flink.util.Collector<RowData> out) throws Exception {
            JsonNode node = mapper.readTree(line);
            if (!node.has("kind")) {
                return; // watermark or placeholder
            }
            out.collect(toRowData(node, rowType, converter));
        }
    }

    static RowData toRowData(JsonNode node, RowType rowType, DynamicTableSource.DataStructureConverter converter) {
        Row row = (Row) external(node.get("values"), rowType);
        row.setKind(kind(node.get("kind").asText()));
        RowData data = (RowData) converter.toInternal(row);
        data.setRowKind(row.getKind());
        return data;
    }

    static RowKind kind(String s) {
        for (RowKind k : RowKind.values()) {
            if (k.shortString().equals(s)) {
                return k;
            }
        }
        throw new IllegalArgumentException("unknown row kind " + s);
    }

    public static final class Lookup extends LookupFunction {
        private final String path;
        private final DynamicTableSource.DataStructureConverter converter;
        private final RowType rowType;
        private final int[][] keys;
        private transient List<RowData> rows;
        private transient RowData.FieldGetter[] keyGetters;
        private transient RowData.FieldGetter[] rowGetters;

        Lookup(String path, DynamicTableSource.DataStructureConverter converter, RowType rowType, int[][] keys) {
            this.path = path;
            this.converter = converter;
            this.rowType = rowType;
            this.keys = keys;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            converter.open(RuntimeConverter.Context.create(Thread.currentThread().getContextClassLoader()));
            ObjectMapper mapper = new ObjectMapper();
            List<RowData> state = new ArrayList<>();
            for (String line : Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8)) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                JsonNode node = mapper.readTree(line);
                if (!node.has("kind")) {
                    continue;
                }
                RowData r = toRowData(node, rowType, converter);
                RowKind kind = r.getRowKind();
                r.setRowKind(RowKind.INSERT);
                if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                    state.add(r);
                } else {
                    state.remove(r);
                }
            }
            rows = state;
            keyGetters = new RowData.FieldGetter[keys.length];
            rowGetters = new RowData.FieldGetter[keys.length];
            for (int i = 0; i < keys.length; i++) {
                LogicalType t = rowType.getTypeAt(keys[i][0]);
                keyGetters[i] = RowData.createFieldGetter(t, i);
                rowGetters[i] = RowData.createFieldGetter(t, keys[i][0]);
            }
        }

        @Override
        public Collection<RowData> lookup(RowData keyRow) {
            List<RowData> out = new ArrayList<>();
            for (RowData r : rows) {
                boolean match = true;
                for (int i = 0; i < keys.length && match; i++) {
                    Object a = keyGetters[i].getFieldOrNull(keyRow);
                    Object b = rowGetters[i].getFieldOrNull(r);
                    match = a != null && a.equals(b);
                }
                if (match) {
                    out.add(r);
                }
            }
            return out;
        }
    }

    // --- sink -------------------------------------------------------------------------------

    static final class Sink implements DynamicTableSink {
        private final String path;
        private final boolean upsert;
        private final DataType type;

        Sink(String path, boolean upsert, DataType type) {
            this.path = path;
            this.upsert = upsert;
            this.type = type;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requested) {
            return upsert ? ChangelogMode.upsert() : ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context ctx) {
            DynamicTableSink.DataStructureConverter converter = ctx.createDataStructureConverter(type);
            String file = path;
            return new DataStreamSinkProvider() {
                @Override
                public DataStreamSink<?> consumeDataStream(ProviderContext pc, DataStream<RowData> ds) {
                    return ds.map(new Capture(file, converter))
                            .setParallelism(1)
                            .sinkTo(new DiscardingSink<>())
                            .setParallelism(1);
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            return new Sink(path, upsert, type);
        }

        @Override
        public String asSummaryString() {
            return "cascade-fixture";
        }
    }

    public static final class Capture extends RichMapFunction<RowData, String> {
        private final String path;
        private final DynamicTableSink.DataStructureConverter converter;
        private transient BufferedWriter writer;

        Capture(String path, DynamicTableSink.DataStructureConverter converter) {
            this.path = path;
            this.converter = converter;
        }

        @Override
        public void open(OpenContext ctx) throws IOException {
            converter.open(RuntimeConverter.Context.create(getRuntimeContext().getUserCodeClassLoader()));
            writer = Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }

        @Override
        public String map(RowData value) throws IOException {
            Row row = (Row) converter.toExternal(value);
            Map<String, Object> enc = new LinkedHashMap<>();
            enc.put("kind", value.getRowKind().shortString());
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < row.getArity(); i++) {
                values.add(Main.encode(row.getField(i)));
            }
            enc.put("values", values);
            writer.write(Json.write(enc));
            writer.newLine();
            writer.flush();
            return "";
        }

        @Override
        public void close() throws IOException {
            if (writer != null) {
                writer.close();
            }
        }
    }

    // --- JSON -> external (Row, String, BigDecimal, LocalDateTime, ...) -----------------------

    static Object external(JsonNode v, LogicalType t) {
        if (v == null || v.isNull()) {
            return null;
        }
        switch (t.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return v.isTextual() ? v.asText() : v.toString();
            case BOOLEAN:
                return v.isBoolean() ? v.asBoolean() : Boolean.parseBoolean(v.asText());
            case TINYINT:
                return (byte) v.asInt();
            case SMALLINT:
                return (short) v.asInt();
            case INTEGER:
                return v.isNumber() ? v.asInt() : Integer.parseInt(v.asText());
            case BIGINT:
                return v.isNumber() ? v.asLong() : Long.parseLong(v.asText());
            case FLOAT:
                return (float) (v.isNumber() ? v.asDouble() : Double.parseDouble(v.asText()));
            case DOUBLE:
                return v.isNumber() ? v.asDouble() : Double.parseDouble(v.asText());
            case DECIMAL:
                return new BigDecimal(v.asText()).setScale(((DecimalType) t).getScale(), java.math.RoundingMode.HALF_UP);
            case DATE:
                return LocalDate.parse(v.asText());
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.parse(v.asText());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return localDateTime(v.asText());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return instant(v.asText());
            case BINARY:
            case VARBINARY:
                return Base64.getDecoder().decode(v.asText());
            case ARRAY: {
                LogicalType elem = ((ArrayType) t).getElementType();
                Class<?> cls = TypeConversions.fromLogicalToDataType(elem).getConversionClass();
                Object arr = java.lang.reflect.Array.newInstance(cls, v.size());
                for (int i = 0; i < v.size(); i++) {
                    java.lang.reflect.Array.set(arr, i, external(v.get(i), elem));
                }
                return arr;
            }
            case MAP:
            case MULTISET: {
                LogicalType kt = t instanceof MapType ? ((MapType) t).getKeyType() : ((MultisetType) t).getElementType();
                LogicalType vt = t instanceof MapType ? ((MapType) t).getValueType() : null;
                Map<Object, Object> m = new HashMap<>();
                Iterator<Map.Entry<String, JsonNode>> it = v.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    Object key = external(new ObjectMapper().getNodeFactory().textNode(e.getKey()), kt);
                    m.put(key, vt == null ? e.getValue().asInt() : external(e.getValue(), vt));
                }
                return m;
            }
            case ROW: {
                RowType rt = (RowType) t;
                Row r = new Row(rt.getFieldCount());
                for (int i = 0; i < rt.getFieldCount(); i++) {
                    JsonNode f = v.isArray() ? v.get(i) : v.get(rt.getFieldNames().get(i));
                    r.setField(i, external(f, rt.getTypeAt(i)));
                }
                return r;
            }
            case VARIANT:
                // any JSON value: an object, array, string or number in the fixture row
                try {
                    return org.apache.flink.types.variant.BinaryVariantInternalBuilder.parseJson(v.toString(), false);
                } catch (java.io.IOException e) {
                    throw new IllegalArgumentException("fixture value " + v + " is not valid JSON for VARIANT", e);
                }
            default:
                throw new IllegalArgumentException("fixture values of type " + t + " are not supported");
        }
    }

    static LocalDateTime localDateTime(String s) {
        String x = s.trim().replace(' ', 'T');
        if (x.endsWith("Z") || x.matches(".*[+-]\\d\\d:\\d\\d$")) {
            return OffsetDateTime.parse(x).withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
        }
        return LocalDateTime.parse(x);
    }

    static Instant instant(String s) {
        return localDateTime(s).toInstant(ZoneOffset.UTC);
    }
}
