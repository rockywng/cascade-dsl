package dev.cascade.tools;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Column-level lineage straight from the planner: for each INSERT of a job script, which columns of
 * which tables every sink column is computed from (Calcite's column-origin metadata, over the logical
 * plan with views expanded, so it names the real source tables).
 *
 * <p>Runs on the unshaded planner ({@code opt/flink-table-planner_2.12}), which must replace the
 * planner loader on the class path: the loader hides Calcite and the planner's operation classes.
 *
 * <pre>
 * {"inserts": [{"sink": "out", "columns": [
 *     {"name": "total", "derived": true, "from": [{"table": "orders", "column": "amount"}]}]}]}
 * </pre>
 * {@code derived} is false when the column is copied unchanged from one source column.
 */
final class Lineage {
    private Lineage() {}

    static Map<String, Object> run(Path script) {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Object> inserts = new ArrayList<>();
        out.put("inserts", inserts);
        try {
            TableEnvironment tEnv = Main.newEnv(null);
            List<String> sql = Main.setup(tEnv, Main.statements(script));
            for (String insert : sql) {
                inserts.add(one((TableEnvironmentImpl) tEnv, insert));
            }
        } catch (Throwable t) {
            out.put("error", Main.errorMessage(t));
        }
        return out;
    }

    private static Map<String, Object> one(TableEnvironmentImpl tEnv, String insert) {
        Map<String, Object> r = new LinkedHashMap<>();
        try {
            List<Operation> ops = tEnv.getParser().parse(insert);
            if (ops.size() != 1 || !(ops.get(0) instanceof SinkModifyOperation)) {
                throw new IllegalArgumentException("not an INSERT: " + insert);
            }
            SinkModifyOperation sink = (SinkModifyOperation) ops.get(0);
            r.put("sink", sink.getContextResolvedTable().getIdentifier().getObjectName());
            QueryOperation child = sink.getChild();
            if (!(child instanceof PlannerQueryOperation)) {
                throw new IllegalStateException("the planner gave " + child.getClass().getSimpleName());
            }
            RelNode rel = ((PlannerQueryOperation) child).getCalciteTree();
            RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
            List<String> names = new ArrayList<>();
            for (Column c : sink.getContextResolvedTable().getResolvedSchema().getColumns()) {
                if (c.isPhysical()) {
                    names.add(c.getName());
                }
            }
            if (sink.getTargetColumns() != null && sink.getTargetColumns().length > 0) {
                names = new ArrayList<>();
                List<Column> cols = sink.getContextResolvedTable().getResolvedSchema().getColumns();
                for (int[] path : sink.getTargetColumns()) {
                    names.add(cols.get(path[0]).getName());
                }
            }
            List<Object> columns = new ArrayList<>();
            int n = rel.getRowType().getFieldCount();
            for (int i = 0; i < n; i++) {
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("name", i < names.size() ? names.get(i) : rel.getRowType().getFieldList().get(i).getName());
                Set<RelColumnOrigin> origins = mq.getColumnOrigins(rel, i);
                List<Object> from = new ArrayList<>();
                boolean derived = false;
                if (origins == null) {
                    c.put("unknown", true);
                } else {
                    Set<String> seen = new LinkedHashSet<>();
                    for (RelColumnOrigin o : origins) {
                        RelOptTable table = o.getOriginTable();
                        List<String> q = table.getQualifiedName();
                        String column = table.getRowType().getFieldList().get(o.getOriginColumnOrdinal()).getName();
                        String key = q.get(q.size() - 1) + "." + column;
                        if (seen.add(key)) {
                            Map<String, Object> f = new LinkedHashMap<>();
                            f.put("table", q.get(q.size() - 1));
                            f.put("column", column);
                            from.add(f);
                        }
                        derived |= o.isDerived();
                    }
                    derived |= from.size() > 1;
                }
                c.put("derived", derived);
                c.put("from", from);
                columns.add(c);
            }
            r.put("columns", columns);
            // Calcite has no column-origin rule for table functions (windows): a column computed through
            // one reads as unknown or as having no origin, so the result for this INSERT is incomplete
            boolean[] tvf = {false};
            new org.apache.calcite.rel.RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof org.apache.calcite.rel.core.TableFunctionScan) {
                        tvf[0] = true;
                    }
                    super.visit(node, ordinal, parent);
                }
            }.go(rel);
            if (tvf[0]) {
                r.put("incomplete", "a table function (a window) sits between the sources and this sink");
            }
        } catch (Throwable t) {
            r.put("error", Main.errorMessage(t));
        }
        return r;
    }
}
