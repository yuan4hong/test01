# PostgreSQL -> Databricks Delta Tables via Debezium + Zerobus

## Architecture

```
PostgreSQL ──WAL──> Debezium Server ──Redis Stream──> Python Bridge ──Zerobus──> Delta Tables
```

- **Debezium Server** captures CDC from PostgreSQL (handles multiple tables, schema changes, snapshots)
- **Redis Stream** is the lightweight transport (no Kafka/Kinesis needed)
- **Python bridge** reads Redis, flattens Debezium envelopes, pushes to Zerobus
- **Delta tables** are auto-created per source table with flat, queryable columns
- **Schema evolution** is automatic: new PostgreSQL columns are added to Delta tables on the fly

## Prerequisites

- Port-forward to PostgreSQL running on `127.0.0.1:1122`
- Debezium Server downloaded in `debezium/debezium-server/`
- Databricks workspace at `nvidia-edsp-or1.cloud.databricks.com`

## Step 1: Install Redis (one-time)

```bash
sudo apt install redis-server
sudo systemctl start redis
sudo systemctl enable redis
```

## Step 2: Install Python dependencies (one-time)

```bash
pip install redis psycopg2-binary databricks-zerobus-ingest-sdk databricks-sql-connector
```

## Step 3: Create a Databricks Service Principal (one-time)

1. Go to **Account Console** > **Service Principals** > **Create**
2. Copy the **Application (Client) ID** and generate a **Secret**
3. Save these for the `.env` file

## Step 4: Get your Zerobus endpoint (one-time)

Contact your Databricks account rep to enable Zerobus Ingest (Public Preview).
They will provide an endpoint in the format:

```
<workspace-id>.zerobus.<region>.cloud.databricks.com
```

## Step 5: Grant permissions in Databricks (one-time)

Run these in a Databricks SQL warehouse (see `databricks_setup.sql`):

```sql
GRANT USE CATALOG ON CATALOG `edsp-dcartm` TO `<sp-application-id>`;
GRANT USE SCHEMA ON SCHEMA `edsp-dcartm`.schema1 TO `<sp-application-id>`;
GRANT CREATE TABLE ON SCHEMA `edsp-dcartm`.schema1 TO `<sp-application-id>`;
```

Replace `<sp-application-id>` with the Client ID from Step 3.

## Step 6: Configure environment and create PostgreSQL publication (one-time)

```bash
cd debezium
cp .env.template .env
# Edit .env -- fill in:
#   ZEROBUS_SERVER_ENDPOINT  (from Step 4)
#   DATABRICKS_CLIENT_ID     (from Step 3)
#   DATABRICKS_CLIENT_SECRET (from Step 3)
#   DATABRICKS_TOKEN         (your personal access token)
source .env
python debezium_zerobus_bridge.py setup
```

This creates `CREATE PUBLICATION dbz_publication FOR ALL TABLES` on PostgreSQL
and verifies `wal_level = logical`.

## Step 7: Run (3 terminals)

**Terminal 1** -- Port-forward to PostgreSQL (you likely already have this):

```bash
tsh proxy app oci-hsg-maestro-db-ro --port 1122
```

**Terminal 2** -- Start Debezium Server:

```bash
cd debezium/debezium-server
./run.sh
```

**Terminal 3** -- Start the bridge:

```bash
cd debezium
source .env
python debezium_zerobus_bridge.py run
```

## Step 8: Query your data

```sql
SELECT * FROM `edsp-dcartm`.schema1.slurm_nodes;
SELECT * FROM `edsp-dcartm`.schema1.slurm_reservations;
```

## Useful commands

Check Redis connectivity and see Debezium streams:

```bash
python debezium_zerobus_bridge.py check
```

## How schema changes work

1. Someone adds a column to a PostgreSQL table -- **nothing breaks**
2. Debezium detects it automatically -- **nothing to do**
3. Bridge sees the new column, runs `ALTER TABLE ADD COLUMN` on the Delta table -- **automatic**
4. Old rows have `NULL` for the new column, new rows have the value

## Files

| File | Purpose |
|------|---------|
| `debezium_zerobus_bridge.py` | Python bridge: Redis consumer, schema manager, Zerobus writer |
| `debezium-server/config/application.properties` | Debezium Server config: PostgreSQL source, Redis sink |
| `.env.template` | Environment variables template (copy to `.env`) |
| `databricks_setup.sql` | One-time GRANT statements for the service principal |
