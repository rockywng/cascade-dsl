package dev.cascade.runner;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@code -- @@ensure-table {...}}: creates a Paimon table (database too) in its filesystem warehouse if
 * it doesn't exist, with the declared columns and key. Paimon tables are read and written by path, which
 * only works for existing tables; this makes the first deployment create them.
 */
public final class Tables {
    public static final String ENSURE = "-- @@ensure-table";
    private static final ObjectMapper JSON = new ObjectMapper();

    private Tables() {}

    public static boolean isEnsure(String stmt) {
        return stmt.startsWith(ENSURE);
    }

    @SuppressWarnings("unchecked")
    public static void ensure(TableEnvironment tEnv, String stmt) throws Exception {
        Map<String, Object> spec = JSON.readValue(stmt.substring(ENSURE.length()).trim(), Map.class);
        Map<String, String> catalog = (Map<String, String>) spec.get("catalog");
        String name = "cascade_paimon_" + Integer.toHexString(catalog.get("warehouse").hashCode());
        if (!Arrays.asList(tEnv.listCatalogs()).contains(name)) {
            tEnv.executeSql("CREATE CATALOG `" + name + "` WITH ('type' = 'paimon', 'warehouse' = '"
                    + catalog.get("warehouse").replace("'", "''") + "')");
        }
        String db = "`" + name + "`.`" + spec.get("database") + "`";
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + db);
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS " + db + ".`" + spec.get("table") + "` (");
        List<List<String>> columns = (List<List<String>>) spec.get("columns");
        for (int i = 0; i < columns.size(); i++) {
            ddl.append(i == 0 ? "" : ", ").append('`').append(columns.get(i).get(0)).append("` ")
                    .append(columns.get(i).get(1));
        }
        List<String> pk = (List<String>) spec.get("primary_key");
        if (pk != null && !pk.isEmpty()) {
            ddl.append(", PRIMARY KEY (`").append(String.join("`, `", pk)).append("`) NOT ENFORCED");
        }
        ddl.append(")");
        Map<String, String> options = (Map<String, String>) spec.get("options");
        if (options != null && !options.isEmpty()) {
            ddl.append(" WITH (");
            int i = 0;
            for (Map.Entry<String, String> e : options.entrySet()) {
                ddl.append(i++ == 0 ? "" : ", ").append("'").append(e.getKey()).append("' = '")
                        .append(e.getValue().replace("'", "''")).append("'");
            }
            ddl.append(")");
        }
        tEnv.executeSql(ddl.toString());
    }
}
