package dev.cascade.runner;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

/**
 * Pre-start checks cascade puts in a job script ({@code -- @@require-slot {...}}).
 *
 * <p>A Postgres CDC source restored from a savepoint resumes from the LSN its replication slot
 * holds. If the slot is gone (database failover, someone "cleaning up"), Flink CDC silently creates
 * a new one at the current position: every change made in between is lost, with no error (found by
 * integration/soak.py --cdc-edge-cases). So on restore the slot must still exist.
 */
public final class Guards {
    public static final String REQUIRE_SLOT = "-- @@require-slot";
    private static final ObjectMapper JSON = new ObjectMapper();

    private Guards() {}

    public static boolean isGuard(String stmt) {
        return stmt.startsWith(REQUIRE_SLOT);
    }

    @SuppressWarnings("unchecked")
    public static void check(String stmt, boolean restoring, Map<String, String> env) throws Exception {
        if (!restoring || !isGuard(stmt)) {
            return; // a fresh start snapshots the table and creates the slot: nothing can be missed
        }
        Map<String, Object> spec = JSON.readValue(stmt.substring(REQUIRE_SLOT.length()).trim(), Map.class);
        String slot = (String) spec.get("slot");
        String source = (String) spec.get("source");
        if ("1".equals(env.get("CASCADE_ALLOW_SLOT_LOSS"))) {
            System.err.println("cascade: CASCADE_ALLOW_SLOT_LOSS=1, not checking replication slot " + slot);
            return;
        }
        String url = "jdbc:postgresql://" + spec.get("hostname") + ":" + spec.get("port") + "/" + spec.get("database");
        Properties props = new Properties();
        props.setProperty("user", String.valueOf(spec.get("username")));
        Object pwEnv = spec.get("password_env");
        String pw = pwEnv != null ? env.get(String.valueOf(pwEnv)) : (String) spec.get("password");
        if (pw != null) {
            props.setProperty("password", pw);
        }
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(url, props);
             PreparedStatement ps = c.prepareStatement("SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slot);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            "replication slot '" + slot + "' of CDC source '" + source + "' is gone. Restoring would "
                            + "resume from a new slot at the current position and silently lose every change "
                            + "since the savepoint. Start this source fresh instead (deploy without the savepoint, "
                            + "or backfill the models that read it), or set CASCADE_ALLOW_SLOT_LOSS=1 to accept "
                            + "the gap. (If the source is new in this version, deploy with `cascade plan "
                            + "--previous`, which knows that it has no slot yet.)");
                }
            }
        }
    }
}
