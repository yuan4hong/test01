#!/usr/bin/env python3
"""
Debezium -> Zerobus Ingest Bridge

Reads CDC events from Redis Stream (fed by Debezium Server) and pushes them
into Databricks Delta tables via the Zerobus Ingest SDK.

Architecture:
  PostgreSQL ──WAL──> Debezium Server ──Redis Stream──> This Script ──Zerobus──> Delta Tables

Each PostgreSQL source table gets its own Delta table with flat, queryable
columns. Schema evolution is automatic: when PostgreSQL adds a column,
Debezium includes it, and the bridge adds it to the Delta table via
ALTER TABLE before ingesting.

Result: SELECT * FROM slurm_nodes just works.

Prerequisites:
  1. PostgreSQL: wal_level=logical, publication created (see 'setup' mode)
  2. Redis: apt install redis-server && systemctl start redis
  3. pip install redis databricks-zerobus-ingest-sdk databricks-sql-connector
  4. Databricks: service principal + Zerobus endpoint (see .env.template)
  5. Debezium Server configured with Redis sink (see config/application.properties)

Usage:
  source .env
  # Terminal 1: start Debezium Server
  cd debezium-server && ./run.sh
  # Terminal 2: start bridge
  python debezium_zerobus_bridge.py run
  # One-time PostgreSQL setup
  python debezium_zerobus_bridge.py setup
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from datetime import datetime

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("dbz-zerobus")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

ZEROBUS_SERVER_ENDPOINT = os.environ.get("ZEROBUS_SERVER_ENDPOINT", "")
DATABRICKS_WORKSPACE_URL = os.environ.get("DATABRICKS_WORKSPACE_URL", "")
DATABRICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "")
DATABRICKS_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "")

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")

TOPIC_PREFIX = os.environ.get("DEBEZIUM_TOPIC_PREFIX", "tutorial")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "zerobus-bridge")
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", f"worker-{os.getpid()}")

PG_HOST = os.environ.get("PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("PG_PORT", "1122"))
PG_USER = os.environ.get("PG_USER", "debezium")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "dcartm1234")
PG_DBNAME = os.environ.get("PG_DBNAME", "maestro")
PG_PUBLICATION = os.environ.get("PG_PUBLICATION", "dbz_publication")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
running = True


def _shutdown(sig, _):
    global running
    log.info("Shutdown signal received (%s), draining...", sig)
    running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# ---------------------------------------------------------------------------
# Zerobus: one stream per source table (flat columns, directly queryable)
# ---------------------------------------------------------------------------
_zb_sdk = None
_zb_streams: dict = {}


def init_zerobus():
    global _zb_sdk
    from zerobus.sdk.sync import ZerobusSdk
    _zb_sdk = ZerobusSdk(ZEROBUS_SERVER_ENDPOINT, unity_catalog_url=DATABRICKS_WORKSPACE_URL)
    log.info("Zerobus SDK initialized (endpoint=%s)", ZEROBUS_SERVER_ENDPOINT)


def get_stream(table_name: str):
    """Get or create a Zerobus stream for a source table."""
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

    if table_name in _zb_streams:
        return _zb_streams[table_name]

    full_name = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{table_name}"
    props = TableProperties(full_name)
    opts = StreamConfigurationOptions(record_type=RecordType.JSON)
    stream = _zb_sdk.create_stream(
        DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, props, opts,
    )
    _zb_streams[table_name] = stream
    log.info("Opened Zerobus stream -> %s", full_name)
    return stream


def close_all_streams():
    for name, s in list(_zb_streams.items()):
        try:
            s.close()
            log.info("Closed stream: %s", name)
        except Exception as exc:
            log.warning("Error closing %s: %s", name, exc)
    _zb_streams.clear()


# ---------------------------------------------------------------------------
# Schema manager: auto-create tables, auto-add new columns
# ---------------------------------------------------------------------------
_known_columns: dict[str, set[str]] = {}
_db_cursor = None
_schema_initialized = False


def _get_db_cursor():
    """Lazy Databricks SQL connection for DDL operations (CREATE/ALTER TABLE)."""
    global _db_cursor
    if _db_cursor is not None:
        return _db_cursor
    from databricks import sql as dbsql
    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    _db_cursor = conn.cursor()
    log.info("Databricks SQL connected (for DDL)")
    return _db_cursor


def _ensure_schema_and_grants():
    """Create schema if needed and grant service principal access (once)."""
    global _schema_initialized
    if _schema_initialized:
        return
    cursor = _get_db_cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{DATABRICKS_CATALOG}`.`{DATABRICKS_SCHEMA}`")
    log.info("Ensured schema exists: %s.%s", DATABRICKS_CATALOG, DATABRICKS_SCHEMA)
    cursor.execute(f"GRANT USE CATALOG ON CATALOG `{DATABRICKS_CATALOG}` TO `{DATABRICKS_CLIENT_ID}`")
    cursor.execute(f"GRANT USE SCHEMA ON SCHEMA `{DATABRICKS_CATALOG}`.`{DATABRICKS_SCHEMA}` TO `{DATABRICKS_CLIENT_ID}`")
    log.info("Granted USE CATALOG + USE SCHEMA to SP %s", DATABRICKS_CLIENT_ID)
    _schema_initialized = True


def _full_table_name(table: str) -> str:
    return f"`{DATABRICKS_CATALOG}`.`{DATABRICKS_SCHEMA}`.{table}"


def _load_existing_columns(table: str):
    """Fetch column list from an existing Delta table."""
    cursor = _get_db_cursor()
    try:
        cursor.execute(f"DESCRIBE TABLE {_full_table_name(table)}")
        cols = set()
        for row in cursor.fetchall():
            col_name = row[0]
            if not col_name.startswith("#"):
                cols.add(col_name)
        _known_columns[table] = cols
        log.info("Loaded %d existing columns for %s", len(cols), table)
    except Exception:
        _known_columns[table] = set()


def ensure_table_and_columns(table: str, record: dict):
    """Create table if missing, add new columns if record has unknown fields.

    All data columns are STRING so schema changes never cause type conflicts.
    CDC metadata columns (_op, _ts_ms) are typed appropriately.
    """
    if table not in _known_columns:
        _load_existing_columns(table)

    known = _known_columns[table]
    new_cols = set(record.keys()) - known

    if not known:
        _ensure_schema_and_grants()

        col_defs = []
        for col in sorted(record.keys()):
            if col == "_ts_ms":
                col_defs.append(f"`{col}` BIGINT")
            else:
                col_defs.append(f"`{col}` STRING")

        ddl = f"CREATE TABLE IF NOT EXISTS {_full_table_name(table)} ({', '.join(col_defs)})"
        log.info("Creating table: %s (%d columns)", table, len(col_defs))
        _get_db_cursor().execute(ddl)
        _known_columns[table] = set(record.keys())

        _get_db_cursor().execute(
            f"GRANT MODIFY, SELECT ON TABLE {_full_table_name(table)} TO `{DATABRICKS_CLIENT_ID}`"
        )
        log.info("Granted MODIFY, SELECT on %s to SP", table)

    elif new_cols:
        # Table exists but has new columns -- ALTER TABLE
        cursor = _get_db_cursor()
        for col in sorted(new_cols):
            sql_type = "BIGINT" if col == "_ts_ms" else "STRING"
            alter = f"ALTER TABLE {_full_table_name(table)} ADD COLUMN `{col}` {sql_type}"
            log.info("Adding column: %s.%s (%s)", table, col, sql_type)
            try:
                cursor.execute(alter)
            except Exception as exc:
                if "already exists" in str(exc).lower():
                    pass
                else:
                    raise
        _known_columns[table].update(new_cols)


# ---------------------------------------------------------------------------
# Debezium envelope -> flat record
# ---------------------------------------------------------------------------
def parse_debezium_event(raw: str) -> tuple[str | None, dict | None]:
    """Parse a Debezium CDC envelope and return (table_name, flat_record).

    The flat record has all columns from the 'after' (or 'before' for deletes)
    payload, plus CDC metadata (_op, _ts_ms). This goes directly into a
    per-table Delta table -- no JSON blobs, just normal columns.
    """
    evt = json.loads(raw)
    payload = evt.get("payload", evt)

    op = payload.get("op")
    if op is None:
        return None, None

    source = payload.get("source", {})
    table = source.get("table")
    if not table:
        return None, None

    # Flatten the row data into top-level columns
    if op in ("c", "r", "u"):
        row_data = payload.get("after") or {}
    elif op == "d":
        row_data = payload.get("before") or {}
    else:
        return None, None

    record = {}
    for k, v in row_data.items():
        if isinstance(v, (dict, list)):
            record[k] = json.dumps(v)
        elif v is None:
            record[k] = None
        else:
            record[k] = str(v)

    record["_op"] = op
    record["_ts_ms"] = payload.get("ts_ms")

    return table, record


# ---------------------------------------------------------------------------
# Redis stream helpers
# ---------------------------------------------------------------------------
def discover_streams(r: redis.Redis) -> list[str]:
    keys = []
    for k in r.scan_iter(f"{TOPIC_PREFIX}.*", count=500):
        key = k if isinstance(k, str) else k.decode()
        if r.type(key) in ("stream", b"stream"):
            keys.append(key)
    return sorted(keys)


def ensure_consumer_group(r: redis.Redis, stream_key: str):
    try:
        r.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
        log.info("Created consumer group '%s' on %s", CONSUMER_GROUP, stream_key)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


# ---------------------------------------------------------------------------
# Main bridge loop
# ---------------------------------------------------------------------------
def run_bridge():
    log.info("=" * 60)
    log.info("Debezium -> Zerobus Bridge (flat tables, auto-schema)")
    log.info("=" * 60)
    log.info("Redis           : %s:%s", REDIS_HOST, REDIS_PORT)
    log.info("Zerobus endpoint: %s", ZEROBUS_SERVER_ENDPOINT)
    log.info("Workspace       : %s", DATABRICKS_WORKSPACE_URL)
    log.info("Target          : `%s`.%s.<table>", DATABRICKS_CATALOG, DATABRICKS_SCHEMA)
    log.info("Topic prefix    : %s", TOPIC_PREFIX)
    log.info("Consumer        : %s / %s", CONSUMER_GROUP, CONSUMER_NAME)

    _ensure_schema_and_grants()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    log.info("Connected to Redis")

    init_zerobus()

    stream_keys = discover_streams(r)
    for sk in stream_keys:
        ensure_consumer_group(r, sk)
    log.info("Found %d Debezium stream(s): %s", len(stream_keys), stream_keys)

    if not stream_keys:
        log.warning(
            "No streams yet (prefix=%s). "
            "Start Debezium Server and it will create them. Polling...",
            TOPIC_PREFIX,
        )

    processed = 0
    errors = 0
    tables_seen: set[str] = set()
    last_discover = time.monotonic()
    last_status = time.monotonic()

    try:
        while running:
            now = time.monotonic()
            if now - last_discover > 30:
                for sk in discover_streams(r):
                    if sk not in stream_keys:
                        ensure_consumer_group(r, sk)
                        stream_keys.append(sk)
                        log.info("Discovered new stream: %s", sk)
                last_discover = now

            if not stream_keys:
                time.sleep(2)
                stream_keys = discover_streams(r)
                for sk in stream_keys:
                    ensure_consumer_group(r, sk)
                continue

            try:
                results = r.xreadgroup(
                    CONSUMER_GROUP, CONSUMER_NAME,
                    {sk: ">" for sk in stream_keys},
                    count=1000,
                    block=2000,
                )
            except redis.exceptions.ResponseError as exc:
                log.warning("Redis read error: %s", exc)
                time.sleep(1)
                continue

            if not results:
                continue

            batch_count = 0
            for stream_key, messages in results:
                for msg_id, fields in messages:
                    try:
                        raw = (
                            fields.get("value")
                            or fields.get("payload")
                            or next(iter(fields.values()))
                        )
                        table, record = parse_debezium_event(raw)

                        if table and record:
                            if processed == 0:
                                log.info("First record: table=%s, columns=%s", table, sorted(record.keys()))
                            ensure_table_and_columns(table, record)
                            get_stream(table).ingest_record(record)
                            processed += 1
                            batch_count += 1
                            tables_seen.add(table)

                        r.xack(stream_key, CONSUMER_GROUP, msg_id)

                    except Exception as exc:
                        errors += 1
                        log.error(
                            "Failed msg %s from %s: %s", msg_id, stream_key, exc,
                        )

            # Flush after each batch for durability
            for s in _zb_streams.values():
                s.flush()
            if batch_count > 0:
                log.info("Flushed batch: %d records (total: %d)", batch_count, processed)

            if now - last_status > 60:
                log.info(
                    "Status: %d ingested, %d errors, tables=%s",
                    processed, errors, sorted(tables_seen),
                )
                last_status = now

    finally:
        close_all_streams()

    log.info(
        "Bridge stopped. Total ingested: %d, errors: %d, tables: %s",
        processed, errors, sorted(tables_seen),
    )


# ---------------------------------------------------------------------------
# PostgreSQL setup helper
# ---------------------------------------------------------------------------
def setup_postgres():
    """Create the publication Debezium needs."""
    import psycopg2

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        "SELECT 1 FROM pg_publication WHERE pubname = %s", (PG_PUBLICATION,)
    )
    if cur.fetchone():
        log.info("Publication '%s' already exists", PG_PUBLICATION)
    else:
        cur.execute(f"CREATE PUBLICATION {PG_PUBLICATION} FOR ALL TABLES")
        log.info("Created publication '%s' FOR ALL TABLES", PG_PUBLICATION)

    cur.execute("SHOW wal_level")
    wal_level = cur.fetchone()[0]
    if wal_level != "logical":
        log.warning(
            "wal_level is '%s', must be 'logical'. "
            "Run: ALTER SYSTEM SET wal_level = logical; then restart PostgreSQL.",
            wal_level,
        )
    else:
        log.info("wal_level = logical  (OK)")

    cur.close()
    conn.close()
    log.info("PostgreSQL setup complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(
        description="Debezium -> Zerobus bridge (flat tables, auto-schema)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  run     Start the bridge (reads Redis, writes to Zerobus)
  setup   Create PostgreSQL publication for Debezium CDC
  check   Verify Redis connectivity and list Debezium streams
        """,
    )
    p.add_argument("mode", choices=["run", "setup", "check"])
    args = p.parse_args()

    if args.mode == "setup":
        setup_postgres()
    elif args.mode == "check":
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        log.info("Redis OK at %s:%s", REDIS_HOST, REDIS_PORT)
        streams = discover_streams(r)
        if streams:
            for s in streams:
                length = r.xlen(s)
                log.info("  Stream: %-50s  messages: %d", s, length)
        else:
            log.info("  No Debezium streams found (prefix=%s)", TOPIC_PREFIX)
    elif args.mode == "run":
        run_bridge()


if __name__ == "__main__":
    main()
