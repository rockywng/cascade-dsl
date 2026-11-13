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
        }
        run(yaml, senv);
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
