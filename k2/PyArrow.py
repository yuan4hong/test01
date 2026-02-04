#!/usr/bin/env python3
"""Export tables from PostgreSQL -> Parquet using JSON (handles all data types)
   Then upload to S3 for Databricks registration.
"""

import subprocess
import json
import os
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
CLUSTERS = {
    "OCI_HSG": "oci-hsg-dca-wl-prd-001",
    "GCP_EAST4": "gcp-us-east4-dca-wl-prd-001",
}

TABLES = ["slurm_nodes"]
LIMIT = 10000
BATCH_SIZE = 5000  # Larger batches work better with JSON approach
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
S3_BUCKET = "s3://dcartm-team/hongy/maestro_restore"
OUTPUT_DIR = "/home/jovyan"
NAMESPACE = "maestro"

print("=" * 60)
print("PyArrow Export: PostgreSQL -> Parquet (JSON method)")
print("=" * 60)
print(f"Timestamp: {TIMESTAMP}")
print(f"Clusters: {list(CLUSTERS.keys())}")
print(f"Tables: {TABLES}")
print(f"Limit per table: {LIMIT} rows")
print()


def export_table_to_parquet(cluster_name: str, context: str, table_name: str, limit: int, batch_size: int) -> str:
    """Export a single Postgres table to Parquet using JSON serialization.
    
    This handles ALL Postgres data types correctly (JSON, arrays, bytea, etc.)
    """
    output_file = os.path.join(OUTPUT_DIR, f"{cluster_name}_{table_name}_{TIMESTAMP}.parquet")
    all_rows = []
    num_batches = (limit + batch_size - 1) // batch_size
    
    print(f"Exporting {table_name} from {cluster_name}...")
    print(f"  Context: {context}")
    print(f"  Limit: {limit} rows, Batch size: {batch_size}")
    
    for batch_num in range(num_batches):
        offset = batch_num * batch_size
        batch_limit = min(batch_size, limit - offset)
        
        print(f"  Batch {batch_num + 1}/{num_batches}: Rows {offset + 1}-{offset + batch_limit}...", end=" ", flush=True)
        
        # Use row_to_json to get each row as a JSON object
        # This handles ALL Postgres types correctly
        query = f"""
        SELECT row_to_json(t) 
        FROM (
            SELECT * FROM {table_name} 
            OFFSET {offset} 
            LIMIT {batch_limit}
        ) t
        """
        
        cmd = [
            'kubectl', '--context', context, '-n', NAMESPACE,
            'exec', 'maestro-cluster-1', '--',
            'psql', '-U', 'postgres', '-d', 'maestro',
            '-t', '-A',  # Tuples only, unaligned output
            '-c', query
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"✗ FAILED")
            print(f"    Error: {result.stderr}")
            continue
        
        # Parse JSON lines
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
    
    # Convert to parquet via pandas (handles mixed types better)
    print(f"  Writing parquet ({len(all_rows)} total rows)...")
    import pandas as pd
    
    # Convert to DataFrame - this handles mixed types gracefully
    df = pd.DataFrame(all_rows)
    
    # Convert any remaining complex types (lists, dicts) to JSON strings
    for col in df.columns:
        # Check if column has any list or dict values
        sample = df[col].dropna().head(100)
        has_complex = sample.apply(lambda x: isinstance(x, (list, dict))).any()
        if has_complex:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
    
    # Write to parquet
    df.to_parquet(output_file, index=False)
    
    file_size_kb = os.path.getsize(output_file) / 1024
    print(f"  ✓ Saved: {output_file} ({file_size_kb:.2f} KB)")
    
    return output_file


def upload_to_s3(local_file: str, cluster_name: str, table_name: str) -> str:
    """Upload parquet file to S3."""
    s3_path = f"{S3_BUCKET}/export_{cluster_name}_{TIMESTAMP}/{table_name}.parquet"
    
    print(f"  Uploading to S3: {s3_path}")
    
    cmd = ['aws', 's3', 'cp', local_file, s3_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"S3 upload failed: {result.stderr}")
    
    print(f"  ✓ Uploaded successfully")
    return s3_path


# Main execution - loop over all clusters
all_exported = {}

for cluster_name, context in CLUSTERS.items():
    print()
    print("=" * 60)
    print(f"Processing Cluster: {cluster_name}")
    print("=" * 60)
    
    exported_files = {}
    
    for table in TABLES:
        print()
        print("-" * 40)
        
        try:
            # Step 1: Export to parquet
            local_file = export_table_to_parquet(cluster_name, context, table, LIMIT, BATCH_SIZE)
            
            # Step 2: Upload to S3
            s3_path = upload_to_s3(local_file, cluster_name, table)
            
            exported_files[table] = {
                "local": local_file,
                "s3": s3_path
            }
            
        except Exception as e:
            print(f"  ✗ Failed to export {table}: {e}")
            continue
    
    if exported_files:
        all_exported[cluster_name] = exported_files
        
        # Generate Jupyter notebook for this cluster
        script_dir = os.path.dirname(os.path.abspath(__file__))
        notebook_file = os.path.join(script_dir, f"reg_{cluster_name}_{TIMESTAMP}.ipynb")
        
        cells = []
        
        # Cells for each table
        for table, paths in exported_files.items():
            databricks_table = f"{cluster_name}_maestro_{table}"
            code = [
                f'spark.sql("DROP TABLE IF EXISTS {databricks_table}")\n',
                f'spark.sql("""\n',
                f'    CREATE TABLE {databricks_table}\n',
                f'    USING parquet\n',
                f"    LOCATION '{paths['s3']}'\n",
                f'""")\n',
                f'spark.sql("DESCRIBE {databricks_table}").show(50, truncate=False)\n',
                f'spark.sql("SELECT * FROM {databricks_table} LIMIT 10").show(truncate=False)'
            ]
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": code
            })
        
        notebook = {
            "cells": cells,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        
        with open(notebook_file, 'w') as f:
            json.dump(notebook, f, indent=1)
        
        print(f"\n✓ Generated notebook: {notebook_file}")

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
