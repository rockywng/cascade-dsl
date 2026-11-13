#!/bin/bash
# Primary of the failover pair (integration/connectors.py, case postgres-failover): a replication role,
# the standby's physical slot, and logical slots that wait for the standby (synchronized_standby_slots).
set -e
psql -v ON_ERROR_STOP=1 -U postgres -d app <<'SQL'
CREATE ROLE repl WITH REPLICATION LOGIN PASSWORD 'pw';
SELECT pg_create_physical_replication_slot('standby_slot');
SQL
echo "host replication repl all scram-sha-256" >> "$PGDATA/pg_hba.conf"
