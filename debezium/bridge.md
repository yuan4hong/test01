# PostgreSQL CDC to Databricks Delta Tables

End-to-end Change Data Capture pipeline that replicates PostgreSQL tables into
Databricks Delta tables in near real-time, with automatic schema evolution.

## Architecture

```
PostgreSQL (primary)
    │
    │  1. WAL (Write-Ahead Log)
    │     Every INSERT/UPDATE/DELETE on a published table
    │     is recorded in the WAL as a logical replication event.
    │
    ▼
Debezium Server (Java, runs in K8s pod)
    │
    │  2. Logical Replication via pgoutput plugin
    │     Debezium connects as a replication client,
    │     reads WAL changes through a replication slot,
    │     and converts them into CDC event envelopes.
    │
    ▼
Redis Stream (in-memory, runs in same pod)
    │
    │  3. Lightweight message broker
    │     Each source table gets its own stream key:
    │       tutorial.public.slurm_nodes
    │     Events are appended and consumed via consumer groups.
    │
    ▼
Bridge Script (Python, runs in same pod)
    │
    │  4. Transforms CDC envelopes into flat records,
    │     auto-creates/alters Delta tables as needed,
    │     and pushes data via Zerobus Ingest SDK.
    │
    ▼
Databricks Delta Table
    SELECT * FROM `edsp-dcartm`.zero_oci_hsg.slurm_nodes
```

## How Data Moves Through Each Stage

### Stage 1: PostgreSQL WAL

PostgreSQL's Write-Ahead Log is the source of truth for all data changes.
When `wal_level = logical` is set, the WAL includes enough information to
reconstruct row-level changes (which columns changed, old values, new values).

A **publication** defines which tables participate in CDC:

```sql
CREATE PUBLICATION kratos_cdc_pub_test1 FOR TABLE slurm_nodes;
```

A **replication slot** tracks how far Debezium has read. This ensures no
events are lost even if Debezium restarts -- PostgreSQL retains WAL segments
until the slot consumer acknowledges them.

A dedicated `debezium` PostgreSQL user with `REPLICATION` privilege connects
to the primary instance and streams changes in real-time.

**Required permissions on the `debezium` user:**

| Permission | Target | Reason |
|---|---|---|
| `REPLICATION` | role attribute | Connect as replication client |
| `SELECT` | `public.slurm_nodes` | Read table data during initial snapshot |
| `SELECT`, `UPDATE` | `public.cdc_heartbeat` | Keep replication slot alive |
| `USAGE` | `inventory` schema | Access signal table |
| `ALL` | `inventory.debezium_signal` | Send snapshot/signal commands |

### Stage 2: Debezium Server

Debezium Server is a standalone Java application (Quarkus-based) that runs
the PostgreSQL connector. It connects to the primary PostgreSQL instance via
logical replication and converts WAL events into structured CDC envelopes.

Each CDC event is a JSON envelope:

```json
{
  "before": null,
  "after": {
    "node_id": "cpu-0001",
    "cluster_id": "oci-hsg-cs-001-v3",
    "scontrol_state": ["ALLOCATED"],
    "reason": "",
    "updated_at": "2026-02-21T02:30:32.963939Z",
    "stale": false,
    "last_busy_at": "2026-02-20T18:48:08.000000Z"
  },
  "source": {
    "connector": "postgresql",
    "table": "slurm_nodes",
    "schema": "public",
    "txId": 1053787397,
    "lsn": 5732504106288
  },
  "op": "u",
  "ts_ms": 1771641153479
}
```

**Operation types (`op` field):**

| op | Meaning | Data used |
|----|---------|-----------|
| `r` | Snapshot read (initial load) | `after` |
| `c` | Insert | `after` |
| `u` | Update | `after` |
| `d` | Delete | `before` |

**Key configuration** (`application.properties`):

```properties
# Connect to PostgreSQL primary (read-write instance)
debezium.source.database.hostname=maestro-cluster-rw.maestro.svc.cluster.local
debezium.source.database.port=5432
debezium.source.database.user=debezium

# Use existing publication (do not auto-create)
debezium.source.publication.name=kratos_cdc_pub_test1
debezium.source.publication.autocreate.mode=disabled

# Only capture this table
debezium.source.table.include.list=public.slurm_nodes

# Replication slot management
debezium.source.slot.name=debezium_test2
debezium.source.slot.drop.on.stop=true

# Heartbeat keeps the replication slot alive
debezium.source.heartbeat.interval.ms=10000
debezium.source.heartbeat.action.query=UPDATE cdc_heartbeat SET last_update = now() WHERE id = 1

# Sink: write CDC events to Redis Stream
debezium.sink.type=redis
debezium.sink.redis.address=localhost:6379
```

**Important notes:**

- Debezium must connect to the **primary** (read-write) PostgreSQL instance.
  Read replicas cannot create replication slots.
- `CREATE_REPLICATION_SLOT` can block on busy databases. The timeout is
  increased to 5 minutes via `internal.task.management.timeout.ms=300000`.
- `slot.drop.on.stop=true` cleans up the slot when Debezium stops, preventing
  WAL accumulation.

### Stage 3: Redis Stream

Redis acts as a lightweight, zero-configuration message broker between
Debezium and the Python bridge. No Kafka, no Zookeeper, no schema registry.

Debezium writes each CDC event to a Redis Stream keyed by topic:

```
Stream key:  tutorial.public.slurm_nodes
Entry:       { "<record_key_json>": "<cdc_envelope_json>" }
```

The bridge reads using a **consumer group** (`zerobus-bridge`), which
provides:

- **At-least-once delivery**: messages are tracked per consumer and must be
  explicitly acknowledged (`XACK`).
- **Replay**: if the bridge crashes, unacknowledged messages are redelivered.
- **Multiple consumers**: can scale horizontally (not currently used).

**Useful Redis commands for monitoring:**

```bash
# How many events are in the stream
redis-cli XLEN tutorial.public.slurm_nodes

# Consumer group status
redis-cli XINFO GROUPS tutorial.public.slurm_nodes

# Pending (unacknowledged) messages
redis-cli XPENDING tutorial.public.slurm_nodes zerobus-bridge

# Reset consumer group to re-read from beginning
redis-cli XGROUP DESTROY tutorial.public.slurm_nodes zerobus-bridge
```

### Stage 4: Bridge Script (`debezium_zerobus_bridge.py`)

The Python bridge consumes CDC events from Redis and pushes them into
Databricks Delta tables. It handles three responsibilities:

**4a. Parse CDC envelopes into flat records**

The Debezium envelope is unwrapped. The `after` payload (or `before` for
deletes) is flattened into a simple key-value dict. Complex values (arrays,
nested objects) are JSON-serialized to strings. Two metadata columns are added:

- `_op`: operation type (`r`, `c`, `u`, `d`)
- `_ts_ms`: Debezium event timestamp (milliseconds)

**4b. Auto-manage Delta table schema**

On first encounter of a table, the bridge:

1. Creates the Databricks schema if it doesn't exist
2. Grants `USE CATALOG` and `USE SCHEMA` to the service principal
3. Creates the Delta table with all columns as `STRING` (except `_ts_ms`
   which is `BIGINT`)
4. Grants `MODIFY` and `SELECT` on the table to the service principal

When PostgreSQL adds a new column, Debezium includes it in the next event.
The bridge detects the unknown column and runs `ALTER TABLE ... ADD COLUMN`
automatically. Using `STRING` for all data columns avoids type conflicts
during schema evolution.

**4c. Ingest via Zerobus SDK**

Records are ingested in batches of up to 1000, then flushed. The Zerobus
Ingest SDK handles the high-throughput write path into Delta tables,
authenticated via a Databricks service principal (OAuth client credentials).

The Databricks SQL Connector (using a personal access token) is used
separately for DDL operations (CREATE TABLE, ALTER TABLE, GRANT).

### Stage 5: Databricks Delta Table

The result is a standard Delta table that can be queried with SQL:

```sql
-- All records (append-only event log)
SELECT * FROM `edsp-dcartm`.zero_oci_hsg.slurm_nodes LIMIT 10;

-- Row count
SELECT COUNT(*) FROM `edsp-dcartm`.zero_oci_hsg.slurm_nodes;

-- Current state (deduplicated, latest version of each row)
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY node_id, cluster_id
    ORDER BY _ts_ms DESC
  ) AS rn
  FROM `edsp-dcartm`.zero_oci_hsg.slurm_nodes
)
WHERE rn = 1 AND _op != 'd';
```

The raw table is an **append-only event log** -- every change is a new row.
To get the current state of each row, use the deduplication query above
(or create a view from it).

## Deployment

Everything runs inside a single Kubernetes pod (`debezium-cdc`) in the
`dca-rtm-staging` namespace.

### Pod contents

| Component | Purpose |
|---|---|
| Debezium Server (Java 17) | Reads PostgreSQL WAL, writes to Redis |
| Redis Server | In-memory message broker |
| Bridge Script (Python 3.10) | Reads Redis, writes to Databricks |

### Starting the pipeline

```bash
# 1. SSH into the pod
kubectl exec -it debezium-cdc -n dca-rtm-staging -- bash

# 2. Start Redis (if not already running)
redis-server --daemonize yes

# 3. Start Debezium Server (terminal 1)
cd /home/debezium-server && ./run.sh

# 4. Start the bridge (terminal 2)
python3 /home/debezium_zerobus_bridge.py run
```

### Resetting the pipeline

To re-ingest all data from scratch:

```bash
# 1. Stop the bridge (Ctrl+C)

# 2. Drop the Delta table in Databricks SQL Editor
#    DROP TABLE IF EXISTS `edsp-dcartm`.zero_oci_hsg.slurm_nodes;

# 3. Reset the Redis consumer group
redis-cli XGROUP DESTROY tutorial.public.slurm_nodes zerobus-bridge

# 4. Restart the bridge
python3 /home/debezium_zerobus_bridge.py run
```

To also re-snapshot from PostgreSQL, delete Debezium's offset file and
restart Debezium Server:

```bash
rm /home/debezium-server/data/offsets.dat
cd /home/debezium-server && ./run.sh
```

## Monitoring

```bash
# Redis stream depth (should stay low during steady-state)
redis-cli XLEN tutorial.public.slurm_nodes

# Consumer group lag
redis-cli XPENDING tutorial.public.slurm_nodes zerobus-bridge

# Bridge logs show periodic status
#   Status: 31735 ingested, 0 errors, tables=['slurm_nodes']

# Debezium Server logs show replication status
#   Connected to PostgreSQL, streaming from slot debezium_test2
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `permission denied for table X` | `debezium` user lacks `SELECT` | `GRANT SELECT ON X TO debezium;` (on primary) |
| `permission denied for table cdc_heartbeat` | Missing `SELECT`+`UPDATE` | `GRANT SELECT, UPDATE ON cdc_heartbeat TO debezium;` |
| `CREATE_REPLICATION_SLOT` timeout | Long-running queries on primary | Wait for queries to finish, or increase timeout |
| `Publication autocreation is disabled` | Publication doesn't exist | Create it: `CREATE PUBLICATION name FOR TABLE ...;` |
| Zerobus 401 "not authorized" | Service principal lacks grants | Run `GRANT USE CATALOG`, `GRANT USE SCHEMA`, `GRANT MODIFY, SELECT` |
| Bridge reads messages but count is 0 | Wrong field extraction from Redis | Ensure bridge uses `next(iter(fields.values()))` |
| Delta table has more rows than expected | Duplicate ingestion from retries | Use deduplication query or drop and re-ingest |

## Advanced Topics

### Schema Evolution

The bridge handles schema changes automatically in an **additive-only**
fashion -- it adds new things but never removes.

#### New column added in PostgreSQL

Fully automatic. Debezium includes the new column in the next CDC event.
The bridge detects the unknown column and runs
`ALTER TABLE ... ADD COLUMN <name> STRING` on the Delta table before
ingesting. No downtime, no manual intervention.

#### Column removed from PostgreSQL

Safe, no errors. Debezium simply stops including the column in subsequent
events. The Delta table **keeps the column** -- old rows retain their values,
new rows get `NULL` for it. Historical data is preserved.

To manually clean up:

```sql
ALTER TABLE `edsp-dcartm`.zero_oci_hsg.slurm_nodes DROP COLUMN column_name;
```

#### Column type changed in PostgreSQL

Safe, because all data columns are stored as `STRING` in the Delta table.
A column changing from `INTEGER` to `TEXT` in PostgreSQL has no effect on
the Delta table -- both are serialized as strings by the bridge.

### Multi-Table Replication

The bridge supports multiple tables automatically. To replicate all tables:

1. Change the publication to include all tables:

   ```sql
   ALTER PUBLICATION kratos_cdc_pub_test1 SET ALL TABLES;
   ```

2. Remove the table filter from `application.properties`:

   ```properties
   # Remove or comment out:
   # debezium.source.table.include.list=public.slurm_nodes
   ```

3. Grant the `debezium` user access to all current and future tables:

   ```sql
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
   ```

When a new table is created in PostgreSQL and gets its first data change:

- Debezium picks it up via the publication and writes events to a new Redis
  stream (e.g. `tutorial.public.new_table`)
- The bridge auto-discovers new streams every 30 seconds
- The Delta table is created automatically with matching columns and grants

Each PostgreSQL table maps to its own Delta table in the same Databricks
schema.

### Table Drops

When a table is dropped in PostgreSQL:

- **Debezium**: stops receiving events for that table (DDL events are not
  forwarded to the sink)
- **Redis**: existing stream and messages remain but no new events arrive
- **Bridge**: no errors, simply stops seeing events for that table
- **Delta table**: stays intact with all historical data

The Delta table is **not** automatically dropped. It becomes a historical
archive. To clean up manually:

```sql
DROP TABLE IF EXISTS `edsp-dcartm`.zero_oci_hsg.dropped_table_name;
```

```bash
redis-cli DEL tutorial.public.dropped_table_name
```

### Deduplication

The raw Delta table is an append-only event log. Multiple CDC events for
the same row (e.g. snapshot + subsequent updates) result in multiple rows.
To get the current state of each row, create a view:

```sql
CREATE OR REPLACE VIEW `edsp-dcartm`.zero_oci_hsg.slurm_nodes_current AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY node_id, cluster_id
    ORDER BY _ts_ms DESC
  ) AS rn
  FROM `edsp-dcartm`.zero_oci_hsg.slurm_nodes
)
WHERE rn = 1 AND _op != 'd';
```

The `PARTITION BY` columns should be the primary key of the source table.
The `_op != 'd'` filter excludes deleted rows.

## Files

| File | Description |
|---|---|
| `debezium_zerobus_bridge.py` | Python bridge: Redis -> Databricks |
| `debezium-server/config/application.properties` | Debezium Server configuration |
| `debezium-server/run.sh` | Debezium Server startup script |
