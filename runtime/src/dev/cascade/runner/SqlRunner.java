package dev.cascade.runner;

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Entry point of a deployed cascade job (FlinkDeployment {@code job.entryClass}).
 *
 * <pre>
 *   SqlRunner --setup setup.sql (--plan plan.json | --statements statements.sql) [--validate]
 * </pre>
 *
 * <p>setup.sql holds SET, CREATE FUNCTION and CREATE TEMPORARY TABLE statements; compiled plans
 * reference those tables by name, so the setup always runs first. Statements are separated by a
 * line containing only {@code -- @@cascade}.
 *
 * <p>{@code ${secret:a/b}} placeholders are replaced from the environment variable
 * {@code CASCADE_SECRET_A_B} (the FlinkDeployment maps each one from a Kubernetes Secret), escaped
 * for SQL string literals. Missing secrets fail fast with the full list. Secrets never appear in the
 * plan or the ConfigMap.
 *
 * <p>{@code --validate} runs the setup and plans the job without submitting it.
 */
public final class SqlRunner {

    static final String SEPARATOR = "-- @@cascade";
    static final Pattern SECRET = Pattern.compile("\\$\\{secret:([^}]+)\\}");
    private static final Pattern SET = Pattern.compile("(?s)^SET\\s+'(.*?)'\\s*=\\s*'(.*)'\\s*;?$");

    private SqlRunner() {}

    public static void main(String[] args) throws Exception {
        String setup = null;
        String plan = null;
        String statements = null;
        boolean validate = false;
        boolean await = false;
        String restore = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--setup":
                    setup = args[++i];
                    break;
                case "--plan":
                    plan = args[++i];
                    break;
                case "--statements":
                    statements = args[++i];
                    break;
                case "--setup-b64": // scripts inline, for jobs that can't be given files (session jobs)
                    setup = tempFile("setup", args[++i]);
                    break;
                case "--plan-b64":
                    plan = tempFile("plan", args[++i]);
                    break;
                case "--statements-b64":
                    statements = tempFile("statements", args[++i]);
                    break;
                case "--validate":
                    validate = true;
                    break;
                case "--restore": // savepoint or retained checkpoint (what `flink run -s` passes)
                    restore = args[++i];
                    break;
                case "--await":
                    await = true; // block until the job finishes (batch runs; local tests)
                    break;
                default:
                    throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (setup == null || (plan == null) == (statements == null)) {
            throw new IllegalArgumentException(
                    "usage: SqlRunner --setup setup.sql (--plan plan.json | --statements statements.sql) [--validate]");
        }
        Map<String, String> env = System.getenv();
        List<String> setupStmts = split(substitute(read(setup), env));
        // the planner (streaming or batch) is fixed when the environment is created, so the runtime
        // mode must be known before any SET is applied
        boolean batch = false;
        for (String stmt : setupStmts) {
            Matcher m = SET.matcher(stmt);
            if (m.matches() && m.group(1).equals("execution.runtime-mode")) {
                batch = m.group(2).trim().equalsIgnoreCase("batch");
            }
        }
        EnvironmentSettings.Builder settings = EnvironmentSettings.newInstance();
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        if (restore != null) {
            org.apache.flink.configuration.Configuration rc = new org.apache.flink.configuration.Configuration();
            rc.set(org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH, restore);
            senv.configure(rc);
        }
        if (batch) {
            senv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                senv, (batch ? settings.inBatchMode() : settings.inStreamingMode()).build());
        if (restore != null) { // the table planner builds the job from its own configuration too
            tEnv.getConfig().set(org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH, restore);
        }
        boolean processes = false;
        boolean restoring = !senv.getConfiguration()
                .getOptional(org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH)
                .orElse("").isEmpty();
        // replication slots first: a lost one is refused or moved to a new generation (slot.name)
        Map<String, String> slots = validate ? new java.util.HashMap<>() : Guards.resolve(setupStmts, restoring, env);
        for (String raw : setupStmts) {
            processes |= applySetup(tEnv, Guards.rename(raw, slots));
        }
        if (plan != null) {
            CompiledPlan compiled = tEnv.loadPlan(PlanReference.fromFile(plan));
            if (validate) {
                System.out.println(compiled.explain());
                System.out.println("cascade: plan OK");
                return;
            }
            TableResult result = compiled.execute();
            if (await) {
                result.await();
            }
        } else {
            StreamStatementSet set = tEnv.createStatementSet();
            for (String stmt : split(substitute(read(statements), env))) {
                set.addInsertSql(stripSemicolon(stmt));
            }
            if (validate) {
                System.out.println(set.explain());
                System.out.println("cascade: statements OK");
                return;
            }
            if (processes) {
                // SQL and DataStream parts form one job: attach the inserts to the stream graph
                set.attachAsDataStream();
                String name = tEnv.getConfig().getConfiguration().getString("pipeline.name", "cascade");
                if (await) {
                    senv.execute(name);
                } else {
                    senv.executeAsync(name);
                }
                return;
            }
            TableResult result = set.execute();
            if (await) {
                result.await();
            }
        }
    }

    /**
     * One setup statement (guards were resolved before). Returns true for a process model (DataStream
     * code). Public for the Python runner (cascade_dsl/pyrunner.py), which handles Python process models
     * itself and everything else through here.
     */
    public static boolean applySetup(StreamTableEnvironment tEnv, String stmt) throws Exception {
        Matcher m = SET.matcher(stmt);
        if (Guards.isGuard(stmt)) {
            return false;
        } else if (Tables.isEnsure(stmt)) {
            Tables.ensure(tEnv, stmt);
        } else if (Processes.isDirective(stmt)) {
            Processes.apply(tEnv, stmt); // a process model: DataStream code between SQL views
            return true;
        } else if (m.matches()) {
            tEnv.getConfig().getConfiguration().setString(m.group(1), m.group(2).replace("''", "'"));
        } else {
            tEnv.executeSql(stripSemicolon(stmt));
        }
        return false;
    }

    /** A script read, secrets substituted from the environment, split into statements. */
    public static List<String> readScript(String path, Map<String, String> env) throws Exception {
        return split(substitute(read(path), env));
    }

    static String tempFile(String name, String base64) throws Exception {
        java.nio.file.Path f = Files.createTempFile("cascade-" + name, ".txt");
        Files.write(f, java.util.Base64.getDecoder().decode(base64));
        f.toFile().deleteOnExit();
        return f.toString();
    }

    static String read(String path) throws Exception {
        return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    }

    static List<String> split(String text) {
        List<String> out = new ArrayList<>();
        for (String part : text.split("(?m)^" + Pattern.quote(SEPARATOR) + "\\s*$")) {
            String s = part.trim();
            // drop leading comment lines
            while (s.startsWith("--") && !Processes.isDirective(s) && !Guards.isGuard(s) && !Tables.isEnsure(s)) {
                int nl = s.indexOf('\n');
                s = nl < 0 ? "" : s.substring(nl + 1).trim();
            }
            if (!s.isEmpty()) {
                out.add(s);
            }
        }
        return out;
    }

    static String stripSemicolon(String s) {
        return s.endsWith(";") ? s.substring(0, s.length() - 1) : s;
    }

    /** Environment variable that carries a secret: CASCADE_SECRET_ + name, non-alphanumerics as _. */
    public static String envName(String secret) {
        return "CASCADE_SECRET_" + secret.trim().replaceAll("[^A-Za-z0-9]", "_").toUpperCase();
    }

    /** ${file:name}: where the deployment mounts files (TLS stores), as cascade_dsl.loader.file_path. */
    static final Pattern FILE = Pattern.compile("\\$\\{file:([^}]+)\\}");

    static String files(String text, Map<String, String> env) {
        String dir = env.getOrDefault("CASCADE_FILES_DIR", "/opt/cascade/files");
        Matcher m = FILE.matcher(text);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String name = m.group(1).trim().replaceAll("[^A-Za-z0-9._-]", "_");
            m.appendReplacement(sb, Matcher.quoteReplacement((dir + "/" + name).replace("'", "''")));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static String substitute(String text, Map<String, String> env) {
        text = files(text, env);
        Matcher m = SECRET.matcher(text);
        StringBuffer sb = new StringBuffer();
        TreeSet<String> missing = new TreeSet<>();
        while (m.find()) {
            String value = env.get(envName(m.group(1)));
            if (value == null) {
                missing.add(m.group(1).trim() + " (" + envName(m.group(1)) + ")");
                value = "";
            }
            // values land inside SQL string literals
            m.appendReplacement(sb, Matcher.quoteReplacement(value.replace("'", "''")));
        }
        m.appendTail(sb);
        if (!missing.isEmpty()) {
            throw new IllegalStateException("missing secrets: " + String.join(", ", missing));
        }
        return sb.toString();
    }
}
