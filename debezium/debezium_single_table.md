# Debezium CDC: Full Setup Guide (Holodeck)

Step-by-step guide to replicate PostgreSQL tables from a Kubernetes-hosted
database to Databricks Delta tables using Debezium CDC, Redis, and the
Zerobus bridge.

## Overview

```
Source PostgreSQL (maestro-cluster, maestro namespace)
    │
    │  pg_dump / pg_restore
    ▼
Staging PostgreSQL (postgres-test pod, dca-test-cases namespace)
    │
    │  WAL logical replication
    ▼
Debezium Server (Java, debezium-cdc pod)
    │
    │  CDC events → Redis Stream
    ▼
Bridge Script (Python, same pod)
    │
    │  Zerobus Ingest SDK
    ▼
Databricks Delta Tables (edsp-dcartm.holodeck.*)
```

We use a staging PostgreSQL instance rather than connecting Debezium directly
to the production primary, to avoid any impact on the read-write instance.

## Prerequisites

- `kubectl` access to the target Kubernetes cluster
- Teleport configured: `tsh kube login <cluster-name>`
- Databricks service principal with Client ID and Client Secret
- Databricks Personal Access Token (PAT) for DDL operations

---

## Step 1: Set Kubernetes Context

```bash
tsh kube login aws-us-east-1-dca-wl-prd-005
kubectl config set-context --current --namespace=dca-test-cases
```

## Step 2: Create the Staging PostgreSQL Pod

```yaml
# postgres-test.yaml
apiVersion: v1
kind: Pod
metadata:
  name: postgres-test
  namespace: dca-test-cases
  labels:
    app: postgres-test
spec:
  containers:
  - name: postgres
    image: postgres:16
    args: ["-c", "wal_level=logical"]
    env:
    - name: POSTGRES_PASSWORD
      value: "dcartm1234"
    - name: POSTGRES_DB
      value: "maestro"
    - name: PGDATA
      value: "/var/lib/postgresql/data/pgdata"
    ports:
    - containerPort: 5432
    resources:
      requests:
        cpu: 250m
        memory: 512Mi
      limits:
        memory: 1Gi
    volumeMounts:
    - name: pgdata
      mountPath: /var/lib/postgresql/data
  restartPolicy: Always
  volumes:
  - name: pgdata
    emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: postgres-test
  namespace: dca-test-cases
spec:
  selector:
    app: postgres-test
  ports:
  - port: 5432
    targetPort: 5432
```

```bash
kubectl apply -f postgres-test.yaml
```

> **Warning**: `emptyDir` is ephemeral. Pod restarts will wipe all data.
> For production, use a PersistentVolumeClaim instead.

Key: `wal_level=logical` is set via args, not ALTER SYSTEM, because
ALTER SYSTEM requires a full server restart to take effect.

## Step 3: Copy Data from Source to Staging

### 3a. Create the Debezium worker pod

```yaml
# debezium-cdc.yaml
apiVersion: v1
kind: Pod
metadata:
  name: debezium-cdc
  namespace: dca-test-cases
  labels:
    run: debezium-cdc
spec:
  containers:
  - name: debezium-cdc
    image: ubuntu:22.04
    command: ["sleep", "infinity"]
    resources:
      requests:
        cpu: 500m
        memory: 2Gi
      limits:
        memory: 4Gi
  restartPolicy: Never
```

```bash
kubectl apply -f debezium-cdc.yaml
kubectl exec -it debezium-cdc -n dca-test-cases -- bash
```

### 3b. Install tools on the worker pod

```bash
apt-get update && apt-get install -y \
  wget curl git lsb-release gnupg2 \
  openjdk-17-jre-headless \
  python3 python3-pip python3-venv \
  redis-server

# PostgreSQL 16 client (must match source version)
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  > /etc/apt/sources.list.d/pgdg.list
wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get update && apt-get install -y postgresql-client-16
```

### 3c. Retrieve source credentials

```bash
# Get the maestro user password from the Kubernetes secret
kubectl get secret maestro-cluster-app -n maestro \
  -o jsonpath='{.data.password}' | base64 -d
```

### 3d. pg_dump from source, excluding large tables

Run this from outside the pod (or use kubectl exec piping):

```bash
# Dump to a file on the worker pod (exclude large tables for speed)
kubectl exec debezium-cdc -n dca-test-cases -- bash -c "
  PGPASSWORD='<maestro-password>' pg_dump \
    -h maestro-cluster-rw.maestro.svc.cluster.local \
    -U maestro -d maestro \
    --no-owner --no-privileges \
    --exclude-table=public.script_workflow_events \
    -Fc -f /tmp/maestro_full.dump"
```

### 3e. pg_restore into staging

```bash
kubectl exec debezium-cdc -n dca-test-cases -- bash -c "
  PGPASSWORD='dcartm1234' pg_restore \
    -h postgres-test.dca-test-cases.svc.cluster.local \
    -U postgres -d maestro \
    --no-owner --no-privileges \
    --clean --if-exists \
    /tmp/maestro_full.dump"
```

For the excluded large table, Debezium will handle it via snapshot (Step 7).

## Step 4: Configure PostgreSQL for Debezium

Connect to the staging PostgreSQL:

```bash
kubectl exec postgres-test -n dca-test-cases -- \
  psql -U postgres -d maestro
```

Run the setup SQL:

```sql
-- Debezium user with replication privilege
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dcartm1234';

-- Heartbeat table (keeps replication slot alive)
CREATE TABLE IF NOT EXISTS public.cdc_heartbeat (
  id INTEGER PRIMARY KEY,
  last_update TIMESTAMP WITH TIME ZONE DEFAULT now()
);
INSERT INTO public.cdc_heartbeat (id) VALUES (1) ON CONFLICT DO NOTHING;

-- Signal table (for incremental snapshots)
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE TABLE IF NOT EXISTS inventory.debezium_signal (
  id VARCHAR(42) PRIMARY KEY,
  type VARCHAR(32) NOT NULL,
  data VARCHAR(2048) NULL
);

-- Publication for all tables
CREATE PUBLICATION holodeck_cdc_pub FOR ALL TABLES;

-- Permissions
GRANT CONNECT ON DATABASE maestro TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT USAGE ON SCHEMA inventory TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT SELECT, UPDATE ON public.cdc_heartbeat TO debezium;
GRANT ALL ON inventory.debezium_signal TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
```

Verify `wal_level`:

```sql
SHOW wal_level;  -- must be 'logical'
```

## Step 5: Configure Debezium Server

### 5a. application.properties

Copy to the pod at `/hongy/test01/debezium/debezium-server/config/application.properties`:

```properties
# Sink: Redis Stream
debezium.sink.type=redis
debezium.sink.redis.address=localhost:6379
debezium.sink.redis.memory.threshold.percentage=85
debezium.sink.redis.batch.size=500

# Source: PostgreSQL CDC
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.plugin.name=pgoutput
debezium.source.offset.storage.file.filename=data/offsets.dat
debezium.source.offset.flush.interval.ms=0
debezium.source.database.hostname=postgres-test.dca-test-cases.svc.cluster.local
debezium.source.database.port=5432
debezium.source.database.user=debezium
debezium.source.database.password=dcartm1234
debezium.source.database.dbname=maestro
debezium.source.topic.prefix=holodeck
debezium.source.slot.name=holodeck_slot

# IMPORTANT: keep the slot on stop so restarts resume without re-snapshot
debezium.source.slot.drop.on.stop=false

# Publication (already created in Step 4)
debezium.source.publication.name=holodeck_cdc_pub
debezium.source.publication.autocreate.mode=disabled

# Snapshot mode:
#   initial  = snapshot on first start (no offsets), then stream
#   no_data  = skip snapshot, stream only
debezium.source.snapshot.mode=initial

# To exclude specific tables from snapshot/streaming:
#debezium.source.table.exclude.list=public.script_workflow_events

# To snapshot only specific tables (others stream-only):
#debezium.source.snapshot.include.collection.list=public.script_workflow_events

# Engine timeout for slot creation
debezium.source.internal.task.management.timeout.ms=300000

# Heartbeat keeps replication slot alive
debezium.source.heartbeat.interval.ms=10000
debezium.source.heartbeat.action.query=UPDATE cdc_heartbeat SET last_update = now() WHERE id = 1

# Signal table for incremental snapshots
debezium.source.signal.data.collection=inventory.debezium_signal

# Output: JSON without schema envelope
debezium.format.value=json
debezium.format.value.schemas.enable=false
debezium.format.key=json
debezium.format.key.schemas.enable=false

# Quarkus
quarkus.log.console.json=false
quarkus.kubernetes-config.enabled=false
quarkus.kubernetes-client.trust-certs=false
quarkus.kubernetes-client.namespace=none
```

### 5b. Environment variables (.env)

Create `.env` on the pod at `/hongy/test01/debezium/.env`:

```bash
# Redis
export REDIS_HOST="localhost"
export REDIS_PORT="6379"

# Databricks Zerobus Ingest
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://nvidia-edsp-or1.cloud.databricks.com"

# Service principal credentials
export DATABRICKS_CLIENT_ID="<your-client-id>"
export DATABRICKS_CLIENT_SECRET="<your-client-secret>"

# Target catalog and schema
export DATABRICKS_CATALOG="edsp-dcartm"
export DATABRICKS_SCHEMA="holodeck"

# Topic prefix (must match application.properties)
export DEBEZIUM_TOPIC_PREFIX="holodeck"

# Redis consumer group
export CONSUMER_GROUP="zerobus-bridge"

# PostgreSQL (for bridge setup mode)
export PG_HOST="postgres-test.dca-test-cases.svc.cluster.local"
export PG_PORT="5432"
export PG_USER="debezium"
export PG_PASSWORD="dcartm1234"
export PG_DBNAME="maestro"
export PG_PUBLICATION="holodeck_cdc_pub"

# Databricks SQL (for DDL: CREATE TABLE, ALTER TABLE, GRANT)
export DATABRICKS_HOST="nvidia-edsp-or1.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-databricks-pat>"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/fee15d0c1610eca9"
```

## Step 6: Run the Initial Snapshot (All Tables Except the Largest)

### 6a. Exclude the large table for the first run

In `application.properties`, uncomment the exclude line:

```properties
debezium.source.table.exclude.list=public.script_workflow_events
```

### 6b. Start the pipeline

```bash
# SSH into the pod
kubectl exec -it debezium-cdc -n dca-test-cases -- bash

# Load environment
source /hongy/test01/debezium/.env

# Start Redis
redis-server --daemonize yes

# Start Debezium Server
cd /hongy/test01/debezium/debezium-server
mkdir -p data
nohup bash run.sh > /tmp/debezium.log 2>&1 &

# Start the bridge
cd /hongy/test01/debezium
nohup python3 debezium_zerobus_bridge.py run > /tmp/bridge.log 2>&1 &
```

### 6c. Monitor progress

```bash
# Debezium log (look for "records sent" and "Processing messages")
tail -f /tmp/debezium.log

# Bridge log (look for "Status: N ingested, 0 errors")
tail -f /tmp/bridge.log

# Redis stream depth per table
redis-cli XLEN holodeck.public.slurm_nodes
```

Wait until the bridge log shows all tables are flushed and the ingested
count stabilizes.

## Step 7: Add the Large Table

Once the initial snapshot is complete and streaming is stable:

### 7a. Update application.properties

Comment out the exclude list and add `snapshot.include.collection.list`
so only the large table is snapshotted (others are already in Databricks):

```properties
# Comment out the exclude
#debezium.source.table.exclude.list=public.script_workflow_events

# Only snapshot this table (others already done)
debezium.source.snapshot.include.collection.list=public.script_workflow_events
```

### 7b. Clear offsets and restart

The offsets must be cleared because the replication slot's WAL position
may have advanced past the point where the large table's data was written.
Clearing offsets triggers a fresh snapshot for the table(s) listed in
`snapshot.include.collection.list`.

```bash
# Stop Debezium
pkill -f 'java.*debezium'
sleep 3

# Clear stale offsets
rm -f /hongy/test01/debezium/debezium-server/data/offsets.dat

# Restart
source /hongy/test01/debezium/.env
cd /hongy/test01/debezium/debezium-server
nohup bash run.sh > /tmp/debezium.log 2>&1 &
```

### 7c. Monitor the large table ingestion

```bash
# Watch Redis stream fill up
redis-cli XLEN holodeck.public.script_workflow_events

# Bridge status (ingested count should climb rapidly)
grep "Status:" /tmp/bridge.log | tail -5

# Consumer group progress (pending → 0 means fully consumed)
redis-cli XINFO GROUPS holodeck.public.script_workflow_events
```

At ~180K records/min, a 709K-row table takes about 4 minutes.

### 7d. Clean up config after ingestion

Once the large table is fully ingested, remove
`snapshot.include.collection.list` so future restarts don't re-snapshot it:

```properties
# Remove or comment out after large table is ingested
#debezium.source.snapshot.include.collection.list=public.script_workflow_events
```

## Step 8: Verify with a New Test Table

Create a new table in the staging PostgreSQL to confirm end-to-end streaming
works via WAL (not snapshot):

```bash
kubectl exec postgres-test -n dca-test-cases -- \
  psql -U postgres -d maestro -c "
    CREATE TABLE public.yh_test_101 (key TEXT PRIMARY KEY, value TEXT);
    INSERT INTO public.yh_test_101 VALUES
      ('hello', 'world'),
      ('foo', 'bar'),
      ('test', 'data');
  "
```

Because the publication uses `FOR ALL TABLES`, the new table is automatically
included. Within ~30 seconds:

1. **Debezium** captures the 3 INSERTs via WAL
2. **Redis** receives 3 messages in stream `holodeck.public.yh_test_101`
3. **Bridge** discovers the new stream, creates the Delta table, ingests rows

Verify:

```bash
# Redis should show 3 messages
redis-cli XLEN holodeck.public.yh_test_101

# Bridge log should show table creation
grep "yh_test_101" /tmp/bridge.log
```

Query in Databricks:

```sql
SELECT * FROM `edsp-dcartm`.holodeck.yh_test_101;
```

## Key Configuration Notes

### slot.drop.on.stop

| Value | Behavior | Use case |
|-------|----------|----------|
| `true` | Slot deleted on stop → WAL recycled → must re-snapshot on restart | Throwaway testing |
| `false` | Slot persists → WAL retained → resume on restart, no re-snapshot | Production |

**Always use `false`** unless you explicitly want a clean slate each time.

### snapshot.mode

| Mode | Behavior |
|------|----------|
| `initial` | Snapshot on first start (no offsets), then stream. On restart with offsets, skip snapshot. |
| `no_data` | Never snapshot. Only stream WAL changes from the current position. |
| `always` | Snapshot on every start, then stream. |

### WAL Retention and Disk

With `slot.drop.on.stop=false`, PostgreSQL retains WAL as long as the slot
exists. To prevent unbounded disk growth:

```sql
-- Cap WAL retention at 10GB (default: unlimited)
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

Monitor slot lag:

```sql
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots;
```

## Monitoring Cheat Sheet

```bash
# Is Debezium running?
pgrep -f 'java.*debezium' && echo "running" || echo "stopped"

# Is the bridge running?
pgrep -f 'debezium_zerobus_bridge' && echo "running" || echo "stopped"

# Debezium: latest activity
tail -5 /tmp/debezium.log

# Bridge: latest status
grep "Status:" /tmp/bridge.log | tail -1

# Redis: stream lengths for all tables
redis-cli KEYS 'holodeck.public.*' | while read k; do
  echo "$k: $(redis-cli XLEN $k)"
done

# Redis: consumer group status (pending=0 means caught up)
redis-cli XINFO GROUPS holodeck.public.script_workflow_events

# PostgreSQL: replication slot status
kubectl exec postgres-test -n dca-test-cases -- \
  psql -U postgres -d maestro -c "SELECT * FROM pg_replication_slots;"
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `offset is no longer available on the server` | WAL recycled (slot was dropped or invalidated) | Delete `offsets.dat`, restart Debezium |
| `NoSuchFileException: data/offsets.dat` | Missing data directory | `mkdir -p data` in debezium-server dir |
| `Could not find or load main class` | Wrong startup command | Use `bash run.sh`, not `java -jar` |
| Bridge not consuming a table | Table not in `tables.csv` with `enabled=1` | Add it to `tables.csv` or remove the file |
| Redis XLEN not decreasing | Normal — Redis Streams don't auto-trim | Use `XINFO GROUPS` to check actual consumption |
| `pg_dump: server version mismatch` | pg_dump client version < server | Install `postgresql-client-16` |
| Debezium re-snapshots on every restart | `slot.drop.on.stop=true` | Change to `false` |
