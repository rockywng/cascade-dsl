package dev.cascade.tools;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JVM side of the cascade CLI. Runs against a Flink distribution's lib/ in local mode (an embedded
 * MiniCluster per job); the Python CLI prepares scripts and reads the JSON results.
 *
 * <pre>
 *   describe      script.sql out.json                exact schemas of all temporary views
 *   run           testsDir out.json timeoutSec       execute test queries on bounded inputs
 *   compile-plan  script.sql out.json                COMPILE PLAN for the script's statement set
 *   restore-test  oldSetup old.json newSetup new.json spDir sec out
 *                                                    run old plan, savepoint, restore new plan
 *
 * Compiled plans reference temporary tables/functions by name only, so executing a plan first runs
 * its setup script (SET, CREATE FUNCTION, CREATE TEMPORARY TABLE) -- the same as a deployment.
 * </pre>
 *
 * Scripts are statements separated by a line containing only "-- @@cascade".
 */
public final class Main {

    private static final String SEPARATOR = "-- @@cascade";
    private static final Pattern SET = Pattern.compile("(?s)^SET\\s+'(.*?)'\\s*=\\s*'(.*)'\\s*;?$");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: describe|run|compile-plan|restore-test ...");
            System.exit(2);
        }
        Map<String, Object> out;
        String outPath;
        switch (args[0]) {
            case "describe":
                out = describe(Paths.get(args[1]));
                outPath = args[2];
                break;
            case "run":
                out = runTests(Paths.get(args[1]), Integer.parseInt(args[3]));
                outPath = args[2];
                break;
            case "compile-plan":
                out = compilePlan(Paths.get(args[1]), args[2]);
                outPath = args[2] + ".result.json";
                break;
            case "restore-test":
                out = restoreTest(Paths.get(args[1]), args[2], Paths.get(args[3]), args[4], args[5],
                        Integer.parseInt(args[6]));
                outPath = args[7];
                break;
            default:
                System.err.println("unknown command " + args[0]);
                System.exit(2);
                return;
        }
        Files.write(Paths.get(outPath), Json.write(out).getBytes(StandardCharsets.UTF_8));
        System.exit(0);
    }

    // ---------------------------------------------------------------------------------------

    private static TableEnvironment newEnv(Configuration extra) {
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", "1");
        conf.setString("restart-strategy.type", "none");
        conf.setString("rest.port", "0");
        if (extra != null) {
            conf.addAll(extra);
        }
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().withConfiguration(conf).build());
    }

    private static List<String> statements(Path script) throws Exception {
        String text = new String(Files.readAllBytes(script), StandardCharsets.UTF_8);
        List<String> out = new ArrayList<>();
        for (String part : text.split("(?m)^" + Pattern.quote(SEPARATOR) + "\\s*$")) {
            String s = part.trim();
            if (!s.isEmpty()) {
                out.add(s);
            }
        }
        return out;
    }

    /** Executes DDL/SET statements; returns INSERT statements for a statement set. */
    private static List<String> setup(TableEnvironment tEnv, List<String> stmts) {
        List<String> inserts = new ArrayList<>();
        for (String s : stmts) {
            Matcher m = SET.matcher(s);
            if (m.matches()) {
                tEnv.getConfig().getConfiguration().setString(m.group(1), m.group(2).replace("''", "'"));
            } else if (s.regionMatches(true, 0, "INSERT", 0, 6)) {
                inserts.add(s.endsWith(";") ? s.substring(0, s.length() - 1) : s);
            } else {
                tEnv.executeSql(s.endsWith(";") ? s.substring(0, s.length() - 1) : s);
            }
        }
        return inserts;
    }

    // --- describe -------------------------------------------------------------------------

    private static Map<String, Object> describe(Path script) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> views = new LinkedHashMap<>();
        try {
            TableEnvironment tEnv = newEnv(null);
            setup(tEnv, statements(script));
            for (String view : tEnv.listTemporaryViews()) {
                try {
                    views.put(view, columns(tEnv.from("`" + view + "`").getResolvedSchema()));
                } catch (Throwable t) {
                    Map<String, Object> err = new LinkedHashMap<>();
                    err.put("error", errorMessage(t));
                    views.put(view, err);
                }
            }
        } catch (Throwable t) {
            out.put("error", errorMessage(t));
        }
        out.put("views", views);
        return out;
    }

    private static List<Object> columns(ResolvedSchema schema) {
        List<Object> cols = new ArrayList<>();
        for (Column c : schema.getColumns()) {
            LogicalType t = c.getDataType().getLogicalType();
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("name", c.getName());
            String timeAttr = null;
            if (LogicalTypeChecks.isRowtimeAttribute(t)) {
                timeAttr = "rowtime";
            } else if (LogicalTypeChecks.isProctimeAttribute(t)) {
                timeAttr = "proctime";
            }
            String type = t.asSummaryString().replace(" *ROWTIME*", "").replace(" *PROCTIME*", "");
            col.put("type", type);
            col.put("time_attribute", timeAttr);
            cols.add(col);
        }
        return cols;
    }

    // --- run tests ------------------------------------------------------------------------

    private static Map<String, Object> runTests(Path dir, int timeoutSec) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Object> results = new ArrayList<>();
        File[] tests = dir.toFile().listFiles(File::isDirectory);
        Arrays.sort(tests);
        ExecutorService pool = Executors.newCachedThreadPool();
        for (File test : tests) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("name", test.getName());
            long start = System.currentTimeMillis();
            TableResult[] holder = new TableResult[1];
            try {
                TableEnvironment tEnv = newEnv(null);
                setup(tEnv, statements(test.toPath().resolve("setup.sql")));
                String query =
                        new String(Files.readAllBytes(test.toPath().resolve("query.sql")), StandardCharsets.UTF_8)
                                .trim();
                Future<List<Object>> rows =
                        pool.submit(
                                () -> {
                                    TableResult result = tEnv.executeSql(query);
                                    holder[0] = result;
                                    r.put("columns", columns(result.getResolvedSchema()));
                                    List<Object> collected = new ArrayList<>();
                                    try (CloseableIterator<Row> it = result.collect()) {
                                        while (it.hasNext()) {
                                            Row row = it.next();
                                            Map<String, Object> enc = new LinkedHashMap<>();
                                            enc.put("kind", row.getKind().shortString());
                                            List<Object> values = new ArrayList<>();
                                            for (int i = 0; i < row.getArity(); i++) {
                                                values.add(encode(row.getField(i)));
                                            }
                                            enc.put("values", values);
                                            collected.add(enc);
                                        }
                                    }
                                    return collected;
                                });
                r.put("rows", rows.get(timeoutSec, TimeUnit.SECONDS));
            } catch (java.util.concurrent.TimeoutException e) {
                r.put("error", "timed out after " + timeoutSec + "s (is a source unbounded?)");
                cancel(holder[0]);
            } catch (Throwable t) {
                r.put("error", errorMessage(t));
                r.put("expectation_violation", hasCause(t, "ExpectationViolation"));
                cancel(holder[0]);
            }
            r.put("millis", System.currentTimeMillis() - start);
            results.add(r);
        }
        pool.shutdownNow();
        out.put("results", results);
        return out;
    }

    private static void cancel(TableResult r) {
        try {
            if (r != null && r.getJobClient().isPresent()) {
                r.getJobClient().get().cancel().get(10, TimeUnit.SECONDS);
            }
        } catch (Throwable ignored) {
            // best effort
        }
    }

    private static Object encode(Object v) {
        if (v == null || v instanceof Boolean || v instanceof Integer || v instanceof Long
                || v instanceof Short || v instanceof Byte || v instanceof String) {
            return v;
        }
        if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            return Double.isFinite(d) ? v : String.valueOf(d);
        }
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).toPlainString();
        }
        if (v instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) v);
        }
        if (v instanceof Row) {
            Row row = (Row) v;
            List<Object> l = new ArrayList<>();
            for (int i = 0; i < row.getArity(); i++) {
                l.add(encode(row.getField(i)));
            }
            return l;
        }
        if (v instanceof Object[]) {
            List<Object> l = new ArrayList<>();
            for (Object o : (Object[]) v) {
                l.add(encode(o));
            }
            return l;
        }
        if (v instanceof Map) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                m.put(String.valueOf(e.getKey()), encode(e.getValue()));
            }
            return m;
        }
        return v.toString(); // LocalDateTime, LocalDate, Instant, ...
    }

    // --- compiled plans ---------------------------------------------------------------------

    private static Map<String, Object> compilePlan(Path script, String outPath) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            TableEnvironment tEnv = newEnv(null);
            List<String> inserts = setup(tEnv, statements(script));
            StatementSet set = tEnv.createStatementSet();
            for (String insert : inserts) {
                set.addInsertSql(insert);
            }
            Files.deleteIfExists(Paths.get(outPath));
            set.compilePlan().writeToFile(outPath);
            out.put("ok", true);
        } catch (Throwable t) {
            out.put("ok", false);
            out.put("error", errorMessage(t));
        }
        return out;
    }

    private static Map<String, Object> restoreTest(
            Path oldSetup, String oldPlan, Path newSetup, String newPlan, String spDir, int seconds)
            throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        JobClient oldJob = null;
        JobClient newJob = null;
        try {
            TableEnvironment t1 = newEnv(null);
            setup(t1, statements(oldSetup));
            TableResult r1 = t1.executePlan(PlanReference.fromFile(oldPlan));
            oldJob = r1.getJobClient().orElseThrow(() -> new IllegalStateException("no job client"));
            waitFor(oldJob, JobStatus.RUNNING, 60);
            Thread.sleep(seconds * 1000L);
            String savepoint =
                    oldJob.stopWithSavepoint(false, new File(spDir).toURI().toString(), SavepointFormatType.CANONICAL)
                            .get(120, TimeUnit.SECONDS);
            out.put("savepoint", savepoint);

            Configuration restore = new Configuration();
            restore.setString("execution.state-recovery.path", savepoint);
            restore.setString("execution.state-recovery.ignore-unclaimed-state", "false");
            TableEnvironment t2 = newEnv(restore);
            setup(t2, statements(newSetup));
            TableResult r2 = t2.executePlan(PlanReference.fromFile(newPlan));
            newJob = r2.getJobClient().orElseThrow(() -> new IllegalStateException("no job client"));
            JobStatus status = waitFor(newJob, JobStatus.RUNNING, 60);
            Thread.sleep(3000); // restore failures surface right after scheduling
            status = newJob.getJobStatus().get(10, TimeUnit.SECONDS);
            out.put("restored", status == JobStatus.RUNNING);
            out.put("status", status.toString());
            if (status != JobStatus.RUNNING) {
                try {
                    newJob.getJobExecutionResult().get(30, TimeUnit.SECONDS);
                } catch (Throwable t) {
                    out.put("error", errorMessage(t));
                }
            }
        } catch (Throwable t) {
            out.put("restored", false);
            out.put("error", errorMessage(t));
        } finally {
            for (JobClient c : new JobClient[] {oldJob, newJob}) {
                if (c != null) {
                    try {
                        c.cancel().get(10, TimeUnit.SECONDS);
                    } catch (Throwable ignored) {
                        // already finished
                    }
                }
            }
        }
        return out;
    }

    private static JobStatus waitFor(JobClient job, JobStatus target, int seconds) throws Exception {
        long deadline = System.currentTimeMillis() + seconds * 1000L;
        JobStatus s = job.getJobStatus().get(10, TimeUnit.SECONDS);
        while (s != target && !s.isGloballyTerminalState() && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
            s = job.getJobStatus().get(10, TimeUnit.SECONDS);
        }
        return s;
    }

    // --- errors -----------------------------------------------------------------------------

    private static String errorMessage(Throwable t) {
        List<String> parts = new ArrayList<>();
        for (Throwable c = t; c != null && parts.size() < 12; c = c.getCause()) {
            String m = c.getClass().getSimpleName() + ": " + c.getMessage();
            if (parts.isEmpty() || !parts.get(parts.size() - 1).equals(m)) {
                parts.add(m);
            }
        }
        return String.join("\n  caused by ", parts);
    }

    private static boolean hasCause(Throwable t, String simpleName) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getClass().getSimpleName().equals(simpleName)) {
                return true;
            }
        }
        return false;
    }
}
