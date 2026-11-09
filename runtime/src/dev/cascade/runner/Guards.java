package dev.cascade.runner;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Replication-slot checks cascade puts in a job script ({@code -- @@require-slot {...}}), run before
 * the job is built.
 *
 * <p>A Postgres CDC source restored from a savepoint resumes from the position its replication slot
 * holds. If the slot is gone (failover, cleanup) or Postgres invalidated it ({@code wal_status = lost},
 * {@code max_slot_wal_keep_size}), Flink CDC silently creates a new one at the current position and
 * every change in between is lost (found by integration/soak.py --cdc-edge-cases). So, on restore:
 * <ul>
 *   <li>{@code on_slot_loss: refuse} (default): refuse to start, naming the slot;
 *   <li>{@code on_slot_loss: resnapshot}: move the source to a new slot generation
 *       ({@code <slot>_g<time>}); the cascade-healing-cdc source sees its saved position belongs to
 *       another generation, re-snapshots, and reconciles downstream state exactly.
 * </ul>
 * On Postgres 17+ new slots are created with {@code failover = true}, so they follow a failover to a
 * standby that syncs slots. Every start logs each slot's WAL status and lag.
 */
public final class Guards {
    public static final String REQUIRE_SLOT = "-- @@require-slot";
    private static final ObjectMapper JSON = new ObjectMapper();

    private Guards() {}

    public static boolean isGuard(String stmt) {
        return stmt.startsWith(REQUIRE_SLOT);
    }

    /** Slot renames (old name -> generation in use) to apply to the setup script. */
    @SuppressWarnings("unchecked")
    public static Map<String, String> resolve(List<String> stmts, boolean restoring, Map<String, String> env)
            throws Exception {
        Map<String, String> renames = new LinkedHashMap<>();
        for (String stmt : stmts) {
            if (!isGuard(stmt)) {
                continue;
            }
            Map<String, Object> spec = JSON.readValue(stmt.substring(REQUIRE_SLOT.length()).trim(), Map.class);
            try {
                String slot = check(spec, restoring, env);
                if (!slot.equals(spec.get("slot"))) {
                    renames.put((String) spec.get("slot"), slot);
                }
            } catch (java.sql.SQLException e) {
                if (restoring) {
                    throw new IllegalStateException("cannot check the replication slot of CDC source '"
                            + spec.get("source") + "': " + e.getMessage(), e);
                }
                System.err.println("cascade: could not check replication slot " + spec.get("slot") + ": " + e);
            }
        }
        return renames;
    }

    /** Applies slot renames to a statement (the source's 'slot.name' option). */
    public static String rename(String stmt, Map<String, String> renames) {
        for (Map.Entry<String, String> e : renames.entrySet()) {
            stmt = stmt.replace("'slot.name' = '" + e.getKey() + "'", "'slot.name' = '" + e.getValue() + "'");
        }
        return stmt;
    }

    private static String check(Map<String, Object> spec, boolean restoring, Map<String, String> env)
            throws Exception {
        String base = (String) spec.get("slot");
        String source = (String) spec.get("source");
        boolean heal = "resnapshot".equals(spec.get("on_slot_loss"));
        boolean allow = "1".equals(env.get("CASCADE_ALLOW_SLOT_LOSS"));
        try (Connection c = connect(spec, env)) {
            int version = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SHOW server_version_num")) {
                if (rs.next()) {
                    version = Integer.parseInt(rs.getString(1).trim());
                }
            }
            // the generations of this source's slot that are still usable, newest last
            List<String> usable = new ArrayList<>();
            List<String> invalid = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT slot_name, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                            + "FROM pg_replication_slots WHERE slot_name = ? OR slot_name LIKE ? ORDER BY slot_name")) {
                ps.setString(1, base);
                ps.setString(2, generationPrefix(base).replace("_", "\\_") + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString(1);
                        String status = rs.getString(2);
                        if ("lost".equals(status)) {
                            invalid.add(name);
                            continue;
                        }
                        usable.add(name);
                        System.err.println("cascade: replication slot " + name + ": wal_status=" + status
                                + ", lag=" + rs.getLong(3) + " bytes");
                        if ("unreserved".equals(status)) {
                            System.err.println("cascade: WARNING replication slot " + name + " is past "
                                    + "max_slot_wal_keep_size: Postgres may invalidate it and the changes in it "
                                    + "would be lost");
                        }
                    }
                }
            }
            for (String name : invalid) {
                System.err.println("cascade: replication slot " + name + " was invalidated by Postgres (wal_status "
                        + "= lost): its changes are gone");
            }
            String current = usable.isEmpty() ? null : newest(base, usable);
            if (current != null) {
                return current;
            }
            if (!restoring) {
                create(c, base, spec, version); // a fresh start snapshots: nothing can be missed
                return base;
            }
            if (heal) {
                drop(c, invalid);
                String next = nextGeneration(base);
                create(c, next, spec, version);
                System.err.println("cascade: replication slot of CDC source '" + source + "' is gone; re-snapshotting "
                        + "it on new slot " + next + " and reconciling downstream state");
                return next;
            }
            if (allow) {
                System.err.println("cascade: CASCADE_ALLOW_SLOT_LOSS=1: restoring " + source + " without its slot");
                return base;
            }
            throw new IllegalStateException(
                    "replication slot '" + base + "' of CDC source '" + source + "' is gone"
                    + (invalid.isEmpty() ? "" : " (invalidated by Postgres)") + ". Restoring would resume from a "
                    + "new slot at the current position and silently lose every change since the savepoint. Set "
                    + "on_slot_loss: resnapshot on the source to heal it, start this source fresh (deploy without "
                    + "the savepoint, or backfill the models that read it), or set CASCADE_ALLOW_SLOT_LOSS=1 to "
                    + "accept the gap. (If the source is new in this version, deploy with `cascade plan "
                    + "--previous`, which knows that it has no slot yet.)");
        }
    }

    private static Connection connect(Map<String, Object> spec, Map<String, String> env) throws Exception {
        String url = "jdbc:postgresql://" + spec.get("hostname") + ":" + spec.get("port") + "/" + spec.get("database");
        Properties props = new Properties();
        props.setProperty("user", String.valueOf(spec.get("username")));
        Object pwEnv = spec.get("password_env");
        String pw = pwEnv != null ? env.get(String.valueOf(pwEnv)) : (String) spec.get("password");
        if (pw != null) {
            props.setProperty("password", pw);
        }
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(url, props);
    }

    /** Creates a logical slot; on Postgres 17+ with failover, so it follows a failover to a synced standby. */
    private static void create(Connection c, String slot, Map<String, Object> spec, int version) throws Exception {
        String plugin = String.valueOf(spec.getOrDefault("plugin", "pgoutput"));
        boolean failover = version >= 170000 && !Boolean.FALSE.equals(spec.get("failover"));
        String sql = failover ? "SELECT pg_create_logical_replication_slot(?, ?, false, false, true)"
                : "SELECT pg_create_logical_replication_slot(?, ?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, slot);
            ps.setString(2, plugin);
            ps.execute();
            System.err.println("cascade: created replication slot " + slot + (failover ? " (failover)" : ""));
        } catch (java.sql.SQLException e) {
            if (!"42710".equals(e.getSQLState())) { // duplicate_object: created concurrently, fine
                throw e;
            }
        }
    }

    private static void drop(Connection c, List<String> slots) {
        for (String s : slots) {
            try (PreparedStatement ps = c.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
                ps.setString(1, s);
                ps.execute();
            } catch (java.sql.SQLException e) {
                System.err.println("cascade: could not drop invalidated slot " + s + ": " + e.getMessage());
            }
        }
    }

    static String generationPrefix(String base) {
        String p = base + "_g";
        return p.length() > 52 ? p.substring(0, 52) : p;
    }

    static String nextGeneration(String base) {
        return generationPrefix(base) + Long.toString(System.currentTimeMillis(), 36); // <= 63 characters
    }

    /** The newest generation: generations sort by their base-36 time suffix; the base slot is oldest. */
    static String newest(String base, List<String> usable) {
        String best = null;
        for (String s : usable) {
            if (s.equals(base)) {
                if (best == null) {
                    best = s;
                }
            } else if (best == null || best.equals(base) || suffix(s).compareTo(suffix(best)) > 0) {
                best = s;
            }
        }
        return best;
    }

    private static String suffix(String s) {
        String t = s.substring(s.lastIndexOf("_g") + 2);
        return String.format("%15s", t); // equal-width compare
    }
}
