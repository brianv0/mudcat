#!/usr/bin/env bash
# Start a local PostgreSQL 16 for development, tests, evals and benchmarks.
# Usage: scripts/dev-db.sh [start|stop|status]
# Data lives in $MUDCAT_PGDATA (default: ./target/pgdata). The server listens on
# port $MUDCAT_PGPORT (default 54329) on localhost and a socket in the data dir.
set -euo pipefail
PGBIN=${PGBIN:-$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1)}
DATA=${MUDCAT_PGDATA:-$(pwd)/target/pgdata}
PORT=${MUDCAT_PGPORT:-54329}
RUN_AS=()
if [ "$(id -u)" = "0" ]; then RUN_AS=(runuser -u postgres --); fi
cmd=${1:-start}
case "$cmd" in
  start)
    if [ ! -f "$DATA/PG_VERSION" ]; then
      mkdir -p "$DATA"
      if [ "$(id -u)" = "0" ]; then chown -R postgres "$DATA"; fi
      "${RUN_AS[@]}" "$PGBIN/initdb" -D "$DATA" -U postgres --auth=trust >/dev/null
      cat >> "$DATA/postgresql.conf" <<CONF
port = $PORT
listen_addresses = 'localhost'
unix_socket_directories = '$DATA'
max_connections = 300
shared_buffers = 1GB
track_commit_timestamp = on
fsync = on
synchronous_commit = off
wal_level = minimal
max_wal_senders = 0
max_wal_size = 4GB
CONF
    fi
    if ! "${RUN_AS[@]}" "$PGBIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
      "${RUN_AS[@]}" "$PGBIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start >/dev/null
    fi
    # the application role is not a superuser, so row-level security applies to it
    psql -h localhost -p "$PORT" -U postgres -qtc "select 1 from pg_roles where rolname='mudcat'" | grep -q 1 \
      || psql -h localhost -p "$PORT" -U postgres -qc "create role mudcat login createdb password 'mudcat'"
    echo "postgres://mudcat:mudcat@localhost:$PORT/mudcat"
    ;;
  stop) "${RUN_AS[@]}" "$PGBIN/pg_ctl" -D "$DATA" -w stop ;;
  status) "${RUN_AS[@]}" "$PGBIN/pg_ctl" -D "$DATA" status ;;
  *) echo "usage: $0 [start|stop|status]" >&2; exit 2 ;;
esac
