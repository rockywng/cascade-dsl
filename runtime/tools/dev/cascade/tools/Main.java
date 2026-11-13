package dev.cascade.tools;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
 *   job-graph     setup.sql plan.json out.json       JobGraph vertices and operators of a plan
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
                        Integer.parseInt(args[6]), args.length > 8 && "allow".equals(args[8]));
                outPath = args[7];
                break;
            case "restore-test-statements":
                out = restoreTestStatements(Paths.get(args[1]), Paths.get(args[2]), args[3],
                        Integer.parseInt(args[4]), args.length > 6 && "allow".equals(args[6]));
                outPath = args[5];
                break;
            case "column-lineage":
                // needs the unshaded planner (opt/flink-table-planner_2.12) instead of the loader: see Lineage
                out = Lineage.run(Paths.get(args[1]));
                outPath = args[2];
                break;
            case "job-graph":
                out = jobGraph(Paths.get(args[1]), args[2]);
                outPath = args[3];
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

    static TableEnvironment newEnv(Configuration extra) {
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", "1");
        conf.setString("restart-strategy.type", "none");
        conf.setString("rest.port", "0");
        conf.setString("table.local-time-zone", "UTC");
        String python = System.getenv("CASCADE_PYTHON");
        if (python != null && !python.isEmpty()) {
            conf.setString("python.executable", python);
            conf.setString("python.client.executable", python);
        }
        if (extra != null) {
            conf.addAll(extra);
        }
        // a StreamTableEnvironment, so process models (DataStream code) can run between SQL views
        return StreamTableEnvironment.create(
                StreamExecutionEnvironment.getExecutionEnvironment(conf),
                EnvironmentSettings.newInstance().inStreamingMode().withConfiguration(conf).build());
    }

    static List<String> statements(Path script) throws Exception {
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
    static List<String> setup(TableEnvironment tEnv, List<String> stmts) throws Exception {
        List<String> inserts = new ArrayList<>();
        for (String s : stmts) {
            Matcher m = SET.matcher(s);
            if (dev.cascade.runner.Guards.isGuard(s)) {
                continue; // local runs use stand-in sources
            } else if (dev.cascade.runner.Tables.isEnsure(s)) {
                dev.cascade.runner.Tables.ensure(tEnv, s); // planning a Paimon table needs it to exist
            } else if (dev.cascade.runner.Processes.isDirective(s)) {
                dev.cascade.runner.Processes.apply((StreamTableEnvironment) tEnv, s);
            } else if (m.matches()) {
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

    /**
     * One test against an environment whose setup is done (the Python runner prepares it, with its
     * Python process models): the query or capture of runTests, as a JSON object.
     */
    public static String runPreparedTest(TableEnvironment tEnv, String dir, int timeoutSec) {
        Map<String, Object> r = new LinkedHashMap<>();
        ExecutorService pool = Executors.newCachedThreadPool();
        TableResult[] holder = new TableResult[1];
        long start = System.currentTimeMillis();
        try {
            Path test = Paths.get(dir);
            String query = new String(Files.readAllBytes(test.resolve("query.sql")), StandardCharsets.UTF_8).trim();
            if (query.endsWith(";")) {
                query = query.substring(0, query.length() - 1);
            }
            Path capture = test.resolve("capture.txt");
            Future<List<Object>> rows;
            if (Files.exists(capture)) {
                List<String> spec = Files.readAllLines(capture, StandardCharsets.UTF_8);
                Path file = Paths.get(spec.get(1).trim());
                r.put("columns", columns(tEnv.from("`" + spec.get(0).trim() + "`").getResolvedSchema()));
                TableResult result = tEnv.executeSql(query);
                holder[0] = result;
                rows = pool.submit(() -> {
                    result.await();
                    return readCapture(file);
                });
            } else {
                TableResult result = tEnv.executeSql(query);
                holder[0] = result;
                r.put("columns", columns(result.getResolvedSchema()));
                rows = pool.submit(() -> {
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
            }
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
        pool.shutdownNow();
        try {
            return new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(r);
        } catch (Exception e) {
            return "{\"error\": \"cannot encode the result\"}";
        }
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
                Path capture = test.toPath().resolve("capture.txt");
                // plan and submit on this thread (Calcite keeps planner metadata in thread-locals);
                // only waiting for results happens on the pool, so it can time out
                Future<List<Object>> rows;
                if (Files.exists(capture)) {
                    List<String> spec = Files.readAllLines(capture, StandardCharsets.UTF_8);
                    Path file = Paths.get(spec.get(1).trim());
                    r.put("columns", columns(tEnv.from("`" + spec.get(0).trim() + "`").getResolvedSchema()));
                    TableResult result =
                            tEnv.executeSql(query.endsWith(";") ? query.substring(0, query.length() - 1) : query);
                    holder[0] = result;
                    rows = pool.submit(() -> {
                        result.await();
                        return readCapture(file);
                    });
                } else {
                    TableResult result = tEnv.executeSql(query);
                    holder[0] = result;
                    r.put("columns", columns(result.getResolvedSchema()));
                    rows = pool.submit(() -> {
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
                }
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

    /** Sink-level test: what the fixture sink recorded, one change per line. */
    private static List<Object> readCapture(Path file) throws Exception {
        List<Object> collected = new ArrayList<>();
        if (Files.exists(file)) {
            org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.trim().isEmpty()) {
                    collected.add(mapper.readValue(line, Map.class));
                }
            }
        }
        return collected;
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

    static Object encode(Object v) {
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
            Path oldSetup, String oldPlan, Path newSetup, String newPlan, String spDir, int seconds,
            boolean allowStateLoss)
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
            // a savepoint, then cancel: stop-with-savepoint ends the job, and the job's local MiniCluster
            // can shut down before the savepoint future resolves ("MiniCluster is not yet running")
            String savepoint =
                    oldJob.triggerSavepoint(new File(spDir).toURI().toString(), SavepointFormatType.CANONICAL)
                            .get(120, TimeUnit.SECONDS);
            try {
                oldJob.cancel().get(30, TimeUnit.SECONDS);
            } catch (Throwable ignored) {
                // already gone
            }
            oldJob = null;
            out.put("savepoint", savepoint);

            Configuration restore = new Configuration();
            restore.setString("execution.state-recovery.path", savepoint);
            restore.setString("execution.state-recovery.ignore-unclaimed-state", String.valueOf(allowStateLoss));
            TableEnvironment t2 = newEnv(restore);
            setup(t2, statements(newSetup));
            TableResult r2 = t2.executePlan(PlanReference.fromFile(newPlan));
            newJob = r2.getJobClient().orElseThrow(() -> new IllegalStateException("no job client"));
            JobStatus status = waitFor(newJob, JobStatus.RUNNING, 60);
            Thread.sleep(3000); // restore failures surface right after scheduling
            try {
                status = newJob.getJobStatus().get(10, TimeUnit.SECONDS);
            } catch (Throwable t) {
                status = JobStatus.FAILED; // its MiniCluster is gone: the job ended; the result says why
            }
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

    /**
     * Translates a compiled plan exactly like EXECUTE PLAN does and returns the resulting JobGraph:
     * vertices (with the IDs Flink reports in metrics and REST) and the operators chained into each.
     */
    private static Map<String, Object> jobGraph(Path setupScript, String planPath) {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            TableEnvironment tEnv = newEnv(null);
            setup(tEnv, statements(setupScript));
            Planner planner = ((TableEnvironmentImpl) tEnv).getPlanner();
            List<Transformation<?>> transformations =
                    planner.translatePlan(planner.loadPlan(PlanReference.fromFile(planPath)));
            Configuration conf = new Configuration();
            conf.addAll((Configuration) tEnv.getConfig().getConfiguration());
            StreamExecutionEnvironment env = new StreamExecutionEnvironment(conf);
            env.configure(tEnv.getConfig().getConfiguration());
            StreamGraph streamGraph = env.generateStreamGraph(transformations);
            JobGraph jobGraph = streamGraph.getJobGraph();
            List<Object> vertices = new ArrayList<>();
            for (JobVertex v : jobGraph.getVerticesSortedTopologicallyFromSources()) {
                Map<String, Object> vertex = new LinkedHashMap<>();
                vertex.put("id", v.getID().toHexString());
                vertex.put("name", v.getName());
                vertex.put("parallelism", v.getParallelism());
                vertex.put("max_parallelism", v.getMaxParallelism());
                List<Object> inputs = new ArrayList<>();
                for (JobEdge e : v.getInputs()) {
                    inputs.add(e.getSource().getProducer().getID().toHexString());
                }
                vertex.put("inputs", inputs);
                List<Object> operators = new ArrayList<>();
                // getOperatorIDs lists the chain from tail to head
                List<OperatorIDPair> ids = new ArrayList<>(v.getOperatorIDs());
                java.util.Collections.reverse(ids);
                for (OperatorIDPair id : ids) {
                    Map<String, Object> op = new LinkedHashMap<>();
                    op.put("id", id.getGeneratedOperatorID().toHexString());
                    op.put("name", id.getUserDefinedOperatorName());
                    op.put("uid", id.getUserDefinedOperatorUid());
                    operators.add(op);
                }
                vertex.put("operators", operators);
                vertices.add(vertex);
            }
            out.put("vertices", vertices);
        } catch (Throwable t) {
            out.put("error", errorMessage(t));
        }
        return out;
    }

    /** Submits a whole script (setup, process directives, inserts) as one job. */
    private static JobClient submitScript(Configuration conf, Path script) throws Exception {
        TableEnvironment tEnv = newEnv(conf);
        List<String> inserts = setup(tEnv, statements(script));
        org.apache.flink.table.api.bridge.java.StreamStatementSet set =
                ((StreamTableEnvironment) tEnv).createStatementSet();
        for (String s : inserts) {
            set.addInsertSql(s);
        }
        set.attachAsDataStream(); // SQL and DataStream (process models) parts as one job
        StreamExecutionEnvironment env =
                ((org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl) tEnv).execEnv();
        return env.executeAsync("cascade-restore-test");
    }

    /** restore-test for pipelines without a compiled plan (process models): whole scripts, strict restore. */
    private static Map<String, Object> restoreTestStatements(Path oldScript, Path newScript, String spDir,
                                                             int seconds, boolean allowStateLoss) {
        Map<String, Object> out = new LinkedHashMap<>();
        JobClient oldJob = null;
        JobClient newJob = null;
        try {
            oldJob = submitScript(null, oldScript);
            waitFor(oldJob, JobStatus.RUNNING, 60);
            Thread.sleep(seconds * 1000L);
            String savepoint = oldJob.triggerSavepoint(new File(spDir).toURI().toString(), SavepointFormatType.CANONICAL)
                    .get(120, TimeUnit.SECONDS);
            out.put("savepoint", savepoint);
            try {
                oldJob.cancel().get(30, TimeUnit.SECONDS);
            } catch (Throwable ignored) {
                // already gone
            }
            oldJob = null;
            Configuration restore = new Configuration();
            restore.setString("execution.state-recovery.path", savepoint);
            restore.setString("execution.state-recovery.ignore-unclaimed-state", String.valueOf(allowStateLoss));
            newJob = submitScript(restore, newScript);
            JobStatus status = waitFor(newJob, JobStatus.RUNNING, 60);
            Thread.sleep(3000);
            try {
                status = newJob.getJobStatus().get(10, TimeUnit.SECONDS);
            } catch (Throwable t) {
                status = JobStatus.FAILED;
            }
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
                        // best effort
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

    static String errorMessage(Throwable t) {
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
