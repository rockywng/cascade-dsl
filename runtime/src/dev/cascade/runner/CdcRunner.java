package dev.cascade.runner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs a replication: a Flink CDC pipeline definition written by cascade ({@code replicate:}).
 *
 * <pre>
 *   CdcRunner --pipeline cdc-pipeline.yaml [--guard guard.sql] [--restore PATH] [--conf key=value ...]
 * </pre>
 *
 * <p>{@code ${secret:...}} placeholders are filled from the environment as for SQL jobs (inside YAML
 * single-quoted strings, so a quote is doubled). The Postgres slot guard runs first: on restore, a lost
 * replication slot refuses the start, because Flink CDC would take a new slot and skip every change in
 * the gap. Then Flink CDC's composer builds the job on this program's execution environment: the
 * application cluster on Kubernetes, a local MiniCluster when run with java. Flink CDC is reached
 * through reflection, so the runtime jar has no compile-time dependency on it.
 */
public final class CdcRunner {
    private CdcRunner() {}

    public static void main(String[] args) throws Exception {
        String pipeline = null;
        String guard = null;
        String restore = null;
        Configuration conf = new Configuration();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--pipeline":
                    pipeline = args[++i];
                    break;
                case "--guard":
                    guard = args[++i];
                    break;
                case "--restore":
                    restore = args[++i];
                    break;
                case "--conf": {
                    String kv = args[++i];
                    int eq = kv.indexOf('=');
                    conf.setString(kv.substring(0, eq), kv.substring(eq + 1));
                    break;
                }
                default:
                    throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (pipeline == null) {
            throw new IllegalArgumentException("usage: CdcRunner --pipeline FILE [--guard FILE] [--restore PATH]");
        }
        if (restore != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, restore);
        }
        // Flink CDC adds the connector jars to the job (pipeline.jars) although they are on the classpath;
        // child-first loading would load their classes twice ("cannot assign Options to Options")
        if (!conf.containsKey("classloader.resolve-order")) {
            conf.setString("classloader.resolve-order", "parent-first");
        }
        Map<String, String> env = System.getenv();
        String yaml = SqlRunner.substitute(
                new String(Files.readAllBytes(Paths.get(pipeline)), StandardCharsets.UTF_8), env);
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        boolean restoring = !senv.getConfiguration().getOptional(StateRecoveryOptions.SAVEPOINT_PATH)
                .orElse("").isEmpty();
        if (guard != null) {
            List<String> guards = new ArrayList<>();
            for (String line : Files.readAllLines(Paths.get(guard), StandardCharsets.UTF_8)) {
                if (Guards.isGuard(line.trim())) {
                    guards.add(line.trim());
                }
            }
            Guards.resolve(guards, restoring, env); // replications refuse a lost slot (no renames)
            for (String g : guards) {
                checkReplicaIdentity(g, env);
            }
        }
        run(yaml, senv);
    }

    /**
     * Flink CDC's Postgres pipeline source needs the before-image of updates and deletes: every table it
     * replicates must have REPLICA IDENTITY FULL, or the job fails at the first update and crash-loops.
     * Checked at every start, for the tables the patterns match now.
     */
    @SuppressWarnings("unchecked")
    static void checkReplicaIdentity(String guard, Map<String, String> env) throws Exception {
        Map<String, Object> spec = new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(guard.substring(Guards.REQUIRE_SLOT.length()).trim(), Map.class);
        Object tables = spec.get("tables");
        if (tables == null) {
            return;
        }
        List<java.util.regex.Pattern[]> include = patterns(String.valueOf(tables));
        List<java.util.regex.Pattern[]> exclude = patterns(String.valueOf(spec.getOrDefault("exclude", "")));
        List<String> offenders = new ArrayList<>();
        try (java.sql.Connection c = Guards.connect(spec, env);
             java.sql.ResultSet rs = c.createStatement().executeQuery(
                     "SELECT n.nspname, c.relname, c.relreplident FROM pg_class c "
                             + "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r', 'p')")) {
            while (rs.next()) {
                String schema = rs.getString(1);
                String table = rs.getString(2);
                if (matches(include, schema, table) && !matches(exclude, schema, table)
                        && !"f".equals(rs.getString(3))) {
                    offenders.add(schema + "." + table);
                }
            }
        }
        if (!offenders.isEmpty()) {
            StringBuilder fix = new StringBuilder();
            for (String t : offenders) {
                fix.append("\n  ALTER TABLE ").append(t).append(" REPLICA IDENTITY FULL;");
            }
            throw new IllegalStateException("replication: these tables lack REPLICA IDENTITY FULL, which Flink CDC "
                    + "needs for updates and deletes (the job would fail at the first one): "
                    + String.join(", ", offenders) + fix);
        }
    }

    /** Flink CDC patterns, comma-separated: database.schema.table, '.' a delimiter and '\\.' any char. */
    static List<java.util.regex.Pattern[]> patterns(String list) {
        List<java.util.regex.Pattern[]> out = new ArrayList<>();
        for (String p : list.split(",")) {
            if (p.trim().isEmpty()) {
                continue;
            }
            String[] parts = p.trim().split("(?<!\\\\)\\.");
            if (parts.length < 2) {
                continue;
            }
            out.add(new java.util.regex.Pattern[] {
                java.util.regex.Pattern.compile(parts[parts.length - 2].replace("\\.", ".")),
                java.util.regex.Pattern.compile(parts[parts.length - 1].replace("\\.", "."))});
        }
        return out;
    }

    static boolean matches(List<java.util.regex.Pattern[]> pats, String schema, String table) {
        for (java.util.regex.Pattern[] p : pats) {
            if (p[0].matcher(schema).matches() && p[1].matcher(table).matches()) {
                return true;
            }
        }
        return false;
    }

    static void run(String yaml, StreamExecutionEnvironment senv) throws Exception {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class<?> parserClass = Class.forName("org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser",
                    true, cl);
            Class<?> cdcConf = Class.forName("org.apache.flink.cdc.common.configuration.Configuration", true, cl);
            Object parser = parserClass.getConstructor().newInstance();
            Object def = parserClass.getMethod("parse", String.class, cdcConf)
                    .invoke(parser, yaml, cdcConf.getConstructor().newInstance());
            Class<?> composerClass = Class.forName("org.apache.flink.cdc.composer.flink.FlinkPipelineComposer",
                    true, cl);
            Object composer = composerClass.getMethod("ofApplicationCluster", StreamExecutionEnvironment.class)
                    .invoke(null, senv);
            Method compose = null;
            for (Method m : composerClass.getMethods()) {
                if (m.getName().equals("compose") && m.getParameterCount() == 1) {
                    compose = m;
                }
            }
            Object execution = compose.invoke(composer, def);
            Object info = execution.getClass().getMethod("execute").invoke(execution);
            System.err.println("cascade: replication finished: " + info);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Flink CDC is not on the classpath: the image needs flink-cdc-dist and "
                    + "the pipeline connector jars (cascade deploy's Dockerfile adds them)", e);
        } catch (InvocationTargetException e) {
            throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
        }
    }
}
