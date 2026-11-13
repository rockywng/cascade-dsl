#!/bin/bash
# Standby of the failover pair: a base backup of the primary, streaming through its physical slot, and
# slot synchronization (sync_replication_slots), so failover-enabled logical slots exist here too.
set -e
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h pgprimary -U postgres; do sleep 1; done
  sleep 3
  PGPASSWORD=pw pg_basebackup -h pgprimary -U repl -D "$PGDATA" -X stream -S standby_slot -R
  # slot sync needs a dbname in primary_conninfo
  echo "primary_conninfo = 'host=pgprimary user=repl password=pw dbname=app'" >> "$PGDATA/postgresql.auto.conf"
  chmod 700 "$PGDATA"
fi
exec postgres -c sync_replication_slots=on -c hot_standby_feedback=on -c wal_level=logical \
  -c max_replication_slots=10 -c max_wal_senders=10
