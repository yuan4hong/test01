#!/usr/bin/env python3
"""Export tables from PostgreSQL -> Databricks directly using SQL connector."""

import subprocess
import json
import os
from datetime import datetime
from databricks import sql

# Databricks SQL connection
db_connection = sql.connect(
    server_hostname="nvidia-edsp-or1.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/fee15d0c1610eca9",
    access_token=os.environ.get("DATABRICKS_TOKEN")
)

print("databricks session id: ", db_connection.get_session_id())
print("connected to databricks.")


# Configuration
CLUSTERS = {
    #"OCI_HSG": "nv-prd-dgxc.teleport.sh-oci-hsg-dca-wl-prd-001",
    "OCI_HSG": "oci-hsg-dca-wl-prd-001",
    "GCP_EAST4": "gcp-us-east4-dca-wl-prd-001",
}

#TABLES = ["slurm_nodes","slurm_reservations","slurm_partitions","slurm_topology_blocks","slurm_nodes_reservations","node_history"]
#TABLES = ["slurm_nodes"]

LIMIT = 100
BATCH_SIZE = 10000
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
K8S_NAMESPACE = "maestro"      # Kubernetes namespace for kubectl
DB_SCHEMA = "schema1"          # Databricks schema for table registration


def get_public_tables(context: str) -> list:
    """Query PostgreSQL for all table names in the public schema."""
    query = (
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public' ORDER BY tablename;"
    )
    cmd = [
        "kubectl", "--context", context, "-n", K8S_NAMESPACE,
        "exec", "maestro-cluster-1", "--",
        "psql", "-U", "postgres", "-d", "maestro",
        "-t", "-A",
        "-c", query,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to list tables: {result.stderr}")
    tables = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
    return tables


# If non-empty, use this list for all clusters; if empty, query public schema per cluster
TABLES = []

print("=" * 60)
print("PostgreSQL -> Databricks Direct Export")
print("=" * 60)
print(f"Timestamp: {TIMESTAMP}")
print(f"Clusters: {list(CLUSTERS.keys())}")
print(f"Tables: {TABLES if TABLES else 'from public schema (per cluster)'}")
print(f"Limit per table: {LIMIT} rows")
print()


def export_table_to_df(cluster_name: str, context: str, table_name: str, limit: int, batch_size: int):
    """Export a single Postgres table to a pandas DataFrame."""
    import pandas as pd
    
    all_rows = []
    num_batches = (limit + batch_size - 1) // batch_size
    
    print(f"Exporting {table_name} from {cluster_name}...")
    print(f"  Context: {context}")
    print(f"  Limit: {limit} rows, Batch size: {batch_size}")
    
    for batch_num in range(num_batches):
        offset = batch_num * batch_size
        batch_limit = min(batch_size, limit - offset)
        
        print(f"  Batch {batch_num + 1}/{num_batches}: Rows {offset + 1}-{offset + batch_limit}...", end=" ", flush=True)
        
        query = f"""
        SELECT row_to_json(t) 
        FROM (
            SELECT * FROM {table_name} 
            order by ctid desc
            OFFSET {offset} 
            LIMIT {batch_limit}
        ) t
        """
        
        cmd = [
            'kubectl', '--context', context, '-n', K8S_NAMESPACE,
            'exec', 'maestro-cluster-1', '--',
            'psql', '-U', 'postgres', '-d', 'maestro',
            '-t', '-A',
            '-c', query
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"✗ FAILED")
            print(f"    Error: {result.stderr}")
            continue
        
        batch_rows = []
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line:
                try:
                    row = json.loads(line)
                    batch_rows.append(row)
                except json.JSONDecodeError as e:
                    print(f"\n    Warning: Failed to parse JSON line: {e}")
                    continue
        
        if batch_rows:
            all_rows.extend(batch_rows)
            print(f"✓ {len(batch_rows)} rows")
        else:
            print(f"✗ No rows parsed")
    
    if not all_rows:
        raise Exception(f"No data exported for {table_name}")
    
    df = pd.DataFrame(all_rows)
    
    # Convert complex types to JSON strings
    for col in df.columns:
        sample = df[col].dropna().head(100)
        has_complex = sample.apply(lambda x: isinstance(x, (list, dict))).any()
        if has_complex:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
    
    print(f"  ✓ Loaded {len(df)} rows into DataFrame")
    return df


def write_df_to_databricks(df, table_name: str):
    """Write DataFrame directly to Databricks using INSERT statements."""
    import pandas as pd
    
    full_table_name = f"`edsp-dcartm`.{DB_SCHEMA}.{table_name}"
    cursor = db_connection.cursor()
    
    print(f"  Creating table: {full_table_name}")
    
    # Drop existing table
    cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    
    # Build CREATE TABLE with columns from DataFrame
    col_defs = []
    for col in df.columns:
        # Infer type from pandas dtype
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "DOUBLE"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        else:
            sql_type = "STRING"
        col_defs.append(f"`{col}` {sql_type}")
    
    create_sql = f"CREATE TABLE {full_table_name} ({', '.join(col_defs)})"
    cursor.execute(create_sql)
    print(f"  ✓ Table created")
    
    # Insert data in batches
    print(f"  Inserting {len(df)} rows...")
    batch_size = 100
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size]
        
        values_list = []
        for _, row in batch_df.iterrows():
            vals = []
            for val in row:
                if pd.isna(val):
                    vals.append("NULL")
                elif isinstance(val, str):
                    escaped = val.replace("'", "''")
                    vals.append(f"'{escaped}'")
                elif isinstance(val, bool):
                    vals.append("TRUE" if val else "FALSE")
                else:
                    vals.append(str(val))
            values_list.append(f"({', '.join(vals)})")
        
        insert_sql = f"INSERT INTO {full_table_name} VALUES {', '.join(values_list)}"
        cursor.execute(insert_sql)
    
    print(f"  ✓ Inserted {len(df)} rows")
    
    # Display first 3 rows
    print(f"  First 3 rows:")
    cursor.execute(f"SELECT * FROM {full_table_name} LIMIT 3")
    for row in cursor.fetchall():
        print(f"    {row}")
    
    cursor.close()


# Main execution
all_exported = {}

for cluster_name, context in CLUSTERS.items():
    print()
    print("=" * 60)
    print(f"Processing Cluster: {cluster_name}")
    print("=" * 60)
    
    tables_to_export = TABLES if TABLES else get_public_tables(context)
    print(f"Tables for {cluster_name}: {tables_to_export}")
    
    exported_tables = []
    
    for table in tables_to_export:
        print()
        print("-" * 40)
        
        try:
            # Step 1: Export from PostgreSQL to DataFrame
            df = export_table_to_df(cluster_name, context, table, LIMIT, BATCH_SIZE)
            
            # Step 2: Write DataFrame directly to Databricks
            table_name = f"{cluster_name}_{table}"
            write_df_to_databricks(df, table_name)
            
            exported_tables.append(table_name)
            
        except Exception as e:
            print(f"  ✗ Failed to export {table}: {e}")
            continue
    
    if exported_tables:
        all_exported[cluster_name] = exported_tables

# Final Summary
print()
print("=" * 60)
print("Final Summary")
print("=" * 60)

if all_exported:
    print(f"✓ Exported from {len(all_exported)}/{len(CLUSTERS)} clusters\n")
    for cluster_name, tables in all_exported.items():
        print(f"  {cluster_name}: {len(tables)} table(s)")
else:
    print("✗ No tables exported successfully")

# Close Databricks connection
db_connection.close()
