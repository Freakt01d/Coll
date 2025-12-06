import os
import subprocess
import glob
import sys
import tempfile
import shutil
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count
import gc
import oracledb

# Database connection details
SCHEMA = "placeholder"
DB_USER = SCHEMA
DB_PASSWORD = "placeholder"
DB_HOST = "placeholder"
DB_PORT = "placeholder"
DB_SID = "placeholder"

# SQL*Loader connection string
DB_CONNECT = f"{DB_USER}/{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_SID}"

# Performance settings (Optimized for 256GB RAM, Xeon Platinum, 10Gbps)
# Each file ~200MB, ~1.14M rows - load entire file in memory
PARALLEL_FILES = 32           # Load 32 files at once (32 x 200MB = 6.4GB, fits easily in 256GB)
ROWS_PER_COMMIT = 2000000     # Load entire file in one commit (no intermediate commits)
DIRECT_PATH = True            # Fast direct path - indexes handled separately
USE_STAGING_TABLES = True     # Use staging tables for lock-free parallel loading
SQLLDR_BINDSIZE = 268435456   # 256MB bind buffer (entire file fits)
SQLLDR_READSIZE = 268435456   # 256MB read buffer

# Table configuration - maps CSV filename pattern to table name
# If CSV filename is "EDS_COMP_001.csv", it will use table EDS_COMP
def get_table_name(csv_file):
    """Extract table name from CSV filename"""
    base = os.path.basename(csv_file)
    name = os.path.splitext(base)[0]
    # Remove numeric suffixes like _001, _002, etc.
    import re
    name = re.sub(r'_\d+$', '', name)
    return name

def get_csv_header(csv_file):
    """Get header row from CSV"""
    with open(csv_file, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
        # Check if it looks like a header (no pure numbers, has text)
        parts = first_line.split(',')
        if parts and not parts[0].replace('.','').replace('-','').isdigit():
            return parts
    return None

def get_table_columns(table_name):
    """Get column names from database table"""
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT column_name, data_type
            FROM all_tab_columns 
            WHERE table_name = :1 AND owner = :2
            ORDER BY column_id
        """, [table_name.upper(), SCHEMA.upper()])
        columns = cursor.fetchall()
        cursor.close()
        connection.close()
        return columns  # List of (column_name, data_type)
    except Exception as e:
        print(f"  Error getting columns: {e}")
        return []

# Date/Timestamp column patterns - add your date columns here
DATE_COLUMNS = {
    'REF_DATE': 'DATE "YYYY-MM-DD HH24:MI:SS"',
    'NEXT_HISTO_DATE': 'TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF6"',
    'LAST_HISTO_DATE': 'TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF6"',
    'TIME_STAMP': 'TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF6"',
    'CREATED_DATE': 'TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF6"',
    'MODIFIED_DATE': 'TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF6"',
    'EFFECTIVE_DATE': 'DATE "YYYY-MM-DD HH24:MI:SS"',
    'END_DATE': 'DATE "YYYY-MM-DD HH24:MI:SS"',
    'START_DATE': 'DATE "YYYY-MM-DD HH24:MI:SS"',
}

def create_control_file(table_name, csv_file, columns, output_dir):
    """Create SQL*Loader control file with proper date/timestamp handling"""
    base = os.path.splitext(os.path.basename(csv_file))[0]
    ctl_file = os.path.join(output_dir, f"{base}.ctl")
    
    # Get columns from database with data types
    db_columns = get_table_columns(table_name)
    db_col_types = {col[0]: col[1] for col in db_columns} if db_columns else {}
    
    # If no columns from CSV header, use database columns
    if not columns and db_columns:
        print(f"    No header in CSV, using {len(db_columns)} columns from database")
        columns = [col[0] for col in db_columns]
    
    # Build column specification with date format handling
    col_specs = []
    if columns:
        for col in columns:
            col_name = col.strip().strip('"').strip("'").upper()
            data_type = db_col_types.get(col_name, '')
            
            # Check data type from database
            if 'TIMESTAMP' in data_type:
                col_specs.append(f'{col_name} TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF6"')
            elif data_type == 'DATE':
                col_specs.append(f'{col_name} DATE "YYYY-MM-DD HH24:MI:SS"')
            elif col_name in DATE_COLUMNS:
                # Fallback to hardcoded list
                col_specs.append(f"{col_name} {DATE_COLUMNS[col_name]}")
            else:
                col_specs.append(col_name)
        col_spec = ',\n    '.join(col_specs)
    else:
        print(f"    WARNING: No columns found for {table_name}!")
        col_spec = "FILLER"  # Fallback - will likely fail but won't crash
    
    control_content = f"""LOAD DATA
INFILE '{csv_file}'
BADFILE '{output_dir}/{base}.bad'
DISCARDFILE '{output_dir}/{base}.dsc'
APPEND
INTO TABLE {SCHEMA}.{table_name}
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
    {col_spec}
)
"""
    
    with open(ctl_file, 'w') as f:
        f.write(control_content)
    
    return ctl_file

def load_single_file(args):
    """Load a single CSV file using SQL*Loader with staging table"""
    csv_file, file_idx, table_name, temp_dir = args
    
    file_name = os.path.basename(csv_file)
    staging_table = None
    
    # Clear memory before starting
    gc.collect()
    
    try:
        # Create staging table for this file
        if USE_STAGING_TABLES:
            staging_table = create_staging_table(table_name, file_idx)
            if not staging_table:
                print(f"  [{file_idx:03d}] ERROR: Could not create staging table")
                return file_idx, False, 0, csv_file, None
            target_table = staging_table
        else:
            target_table = table_name
        
        # Get CSV header
        header_parts = get_csv_header(csv_file)
        
        # Create control file
        ctl_file = create_control_file(target_table, csv_file, header_parts, temp_dir)
        
        # Build sqlldr command
        log_file = os.path.join(temp_dir, f"{os.path.splitext(file_name)[0]}.log")
        
        cmd = [
            'sqlldr',
            f'userid={DB_CONNECT}',
            f'control={ctl_file}',
            f'log={log_file}',
            f'rows={ROWS_PER_COMMIT}',
            'errors=100000',
            'direct_path_lock_wait=true',
            f'bindsize={SQLLDR_BINDSIZE}',
            f'readsize={SQLLDR_READSIZE}',
            'streamsize=16777216',      # 16MB stream buffer
            'multithreading=true',      # Enable multi-threading
            'columnarrayrows=100000',   # Large column array for speed
            'silent=(header,feedback)',
        ]
        
        if DIRECT_PATH:
            cmd.append('direct=true')
        
        print(f"  [{file_idx:03d}] Loading: {file_name} -> {target_table}")
        start_time = datetime.now()
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=14400)
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # Parse log file for row count
        rows_loaded = 0
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                content = f.read()
                import re
                match = re.search(r'(\d+) Rows? successfully loaded', content)
                if match:
                    rows_loaded = int(match.group(1))
        
        success = result.returncode in (0, 2)
        
        if success:
            speed = rows_loaded / elapsed if elapsed > 0 else 0
            print(f"  [{file_idx:03d}] Done: {file_name} - {rows_loaded:,} rows in {elapsed:.1f}s ({speed:,.0f}/s)")
        else:
            print(f"  [{file_idx:03d}] FAILED: {file_name} - check {log_file}")
        
        # Clear memory after processing
        gc.collect()
        
        return file_idx, success, rows_loaded, csv_file, staging_table
        
    except subprocess.TimeoutExpired:
        print(f"  [{file_idx:03d}] TIMEOUT: {file_name}")
        gc.collect()
        return file_idx, False, 0, csv_file, staging_table
    except Exception as e:
        print(f"  [{file_idx:03d}] ERROR: {file_name} - {e}")
        import traceback
        traceback.print_exc()
        gc.collect()
        return file_idx, False, 0, csv_file, staging_table

# Create connection string for oracledb
dest_dsn = oracledb.makedsn(DB_HOST, DB_PORT, sid=DB_SID)
dest_connection_string = f"{DB_USER}/{DB_PASSWORD}@{dest_dsn}"

def init_oracle_client():
    """Initialize Oracle client for thick mode (REQUIRED - database uses encryption)"""
    try:
        # Use specific path to avoid conflicts with multiple Oracle installations
        oracledb.init_oracle_client(lib_dir=r"D:\Homeware\instantclient_23_0")
        print("Oracle thick mode initialized (instantclient_23_0)")
    except oracledb.ProgrammingError:
        # Already initialized - this is OK
        pass
    except Exception as e:
        print(f"ERROR: Oracle thick mode REQUIRED but failed: {e}")
        print("  Check path: D:\\Homeware\\instantclient_23_0")
        sys.exit(1)

def worker_init():
    """Initialize each worker process - MUST call before any DB connection"""
    init_oracle_client()

def truncate_table(table_name):
    """Truncate table using oracledb"""
    full_table = f"{SCHEMA}.{table_name}"
    print(f"Truncating {full_table}...")
    
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {full_table}")
        cursor.close()
        connection.close()
        print(f"  Truncated {full_table}")
        return True
    except Exception as e:
        print(f"  Error: {e}")
        return False

def get_table_indexes(table_name):
    """Get all indexes for a table"""
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT index_name, uniqueness 
            FROM all_indexes 
            WHERE table_name = :1 AND owner = :2
            AND index_type != 'LOB'
        """, [table_name.upper(), SCHEMA.upper()])
        indexes = cursor.fetchall()
        cursor.close()
        connection.close()
        return indexes
    except Exception as e:
        print(f"  Error getting indexes: {e}")
        return []

def disable_indexes(table_name):
    """Make non-unique indexes unusable for faster loading.
    NOTE: Unique indexes CANNOT be disabled for direct path loading!
    """
    indexes = get_table_indexes(table_name)
    if not indexes:
        print(f"  No indexes found on {table_name}")
        return []
    
    # Separate unique and non-unique indexes
    unique_indexes = [(n, u) for n, u in indexes if u == 'UNIQUE']
    nonunique_indexes = [(n, u) for n, u in indexes if u != 'UNIQUE']
    
    if unique_indexes:
        print(f"  Keeping {len(unique_indexes)} UNIQUE indexes enabled (required for direct path)")
        for idx_name, _ in unique_indexes:
            print(f"    Kept: {idx_name}")
    
    if not nonunique_indexes:
        print(f"  No non-unique indexes to disable")
        return []
    
    print(f"  Disabling {len(nonunique_indexes)} non-unique indexes...")
    disabled = []
    
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        
        for idx_name, uniqueness in nonunique_indexes:
            try:
                cursor.execute(f"ALTER INDEX {SCHEMA}.{idx_name} UNUSABLE")
                disabled.append((idx_name, uniqueness))
                print(f"    Disabled: {idx_name}")
            except Exception as e:
                print(f"    Warning: Could not disable {idx_name}: {e}")
        
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"  Error: {e}")
    
    return disabled

def rebuild_indexes(table_name, indexes=None):
    """Rebuild indexes after loading"""
    if indexes is None:
        indexes = get_table_indexes(table_name)
    
    if not indexes:
        return
    
    print(f"  Rebuilding {len(indexes)} indexes on {table_name}...")
    
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        
        for idx_info in indexes:
            idx_name = idx_info[0] if isinstance(idx_info, tuple) else idx_info
            try:
                cursor.execute(f"ALTER INDEX {SCHEMA}.{idx_name} REBUILD PARALLEL")
                cursor.execute(f"ALTER INDEX {SCHEMA}.{idx_name} NOPARALLEL")
                print(f"    Rebuilt: {idx_name}")
            except Exception as e:
                print(f"    Warning: Could not rebuild {idx_name}: {e}")
        
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"  Error: {e}")

def create_staging_table(table_name, chunk_id):
    """Create a staging table for a chunk (copy structure, no data, no indexes)"""
    staging_name = f"{table_name}_STG{chunk_id:02d}"
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        
        # Drop if exists
        try:
            cursor.execute(f"DROP TABLE {SCHEMA}.{staging_name} PURGE")
        except:
            pass
        
        # Create as copy of main table structure (no data, no indexes)
        cursor.execute(f"""
            CREATE TABLE {SCHEMA}.{staging_name} 
            NOLOGGING 
            AS SELECT * FROM {SCHEMA}.{table_name} WHERE 1=0
        """)
        
        cursor.close()
        connection.close()
        return staging_name
    except Exception as e:
        print(f"    Error creating staging table {staging_name}: {e}")
        return None

def merge_staging_tables(table_name, staging_tables):
    """Merge all staging tables into the main table (no parallel to save TEMP space)"""
    print(f"\n  Merging {len(staging_tables)} staging tables into {table_name}...")
    
    merge_count = 0
    total_rows = 0
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        
        for stg_table in staging_tables:
            try:
                # Simple APPEND hint - no PARALLEL to avoid TEMP exhaustion
                cursor.execute(f"""
                    INSERT /*+ APPEND NOLOGGING */ INTO {SCHEMA}.{table_name}
                    SELECT * FROM {SCHEMA}.{stg_table}
                """)
                rows_merged = cursor.rowcount
                connection.commit()  # Commit after each to release TEMP
                
                merge_count += 1
                total_rows += rows_merged
                print(f"    [{merge_count:03d}] Merged {stg_table}: {rows_merged:,} rows")
                
                # Clear memory every 10 merges
                if merge_count % 10 == 0:
                    gc.collect()
                
            except Exception as e:
                print(f"    Error merging {stg_table}: {e}")
                connection.rollback()  # Rollback failed merge
        
        cursor.close()
        connection.close()
        gc.collect()
        
    except Exception as e:
        print(f"  Error in merge: {e}")
    
    print(f"  Total merged: {total_rows:,} rows")
    return merge_count

def drop_staging_tables(staging_tables):
    """Drop all staging tables"""
    print(f"\n  Dropping {len(staging_tables)} staging tables...")
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        
        for stg_table in staging_tables:
            try:
                cursor.execute(f"DROP TABLE {SCHEMA}.{stg_table} PURGE")
            except:
                pass
        
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"  Error dropping staging tables: {e}")

def ensure_indexes_usable(table_name):
    """Ensure all indexes are usable before loading (fix from previous failed runs)"""
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        
        # Find unusable indexes
        cursor.execute("""
            SELECT index_name FROM all_indexes 
            WHERE table_name = :1 AND owner = :2 AND status = 'UNUSABLE'
        """, [table_name.upper(), SCHEMA.upper()])
        unusable = cursor.fetchall()
        
        if unusable:
            print(f"\n  Found {len(unusable)} UNUSABLE indexes - rebuilding...")
            for (idx_name,) in unusable:
                try:
                    cursor.execute(f"ALTER INDEX {SCHEMA}.{idx_name} REBUILD")
                    print(f"    Rebuilt: {idx_name}")
                except Exception as e:
                    print(f"    Warning: {idx_name}: {e}")
        
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"  Error checking indexes: {e}")

def main():
    print("="*80)
    print("SQL*LOADER PARALLEL CSV IMPORT")
    print(f"Loads {PARALLEL_FILES} CSV files in parallel (no splitting)")
    print("="*80 + "\n")
    
    # Initialize Oracle client
    init_oracle_client()
    
    # Get directory
    if len(sys.argv) > 1:
        csv_dir = sys.argv[1]
    else:
        csv_dir = input("Enter directory containing CSV files: ").strip()
        csv_dir = csv_dir.strip('"').strip("'")
    
    if not os.path.exists(csv_dir):
        print(f"ERROR: Directory '{csv_dir}' does not exist!")
        return
    
    # Find CSV files (use set to avoid duplicates on case-insensitive systems)
    csv_files = set(glob.glob(os.path.join(csv_dir, "*.csv")) + 
                    glob.glob(os.path.join(csv_dir, "*.CSV")))
    
    # Exclude staging/chunk files from previous runs
    csv_files = [f for f in csv_files if '_chunk' not in os.path.basename(f).lower() 
                 and '_STG' not in os.path.basename(f)]
    csv_files = sorted(csv_files)
    
    if not csv_files:
        print("No CSV files found!")
        return
    
    print(f"Found {len(csv_files)} CSV files:\n")
    total_size = 0
    for i, f in enumerate(csv_files[:10]):  # Show first 10
        size_gb = os.path.getsize(f) / (1024 * 1024 * 1024)
        total_size += size_gb
        print(f"  {i+1:3d}. {os.path.basename(f)} ({size_gb:.2f} GB)")
    
    if len(csv_files) > 10:
        print(f"  ... and {len(csv_files) - 10} more files")
        for f in csv_files[10:]:
            total_size += os.path.getsize(f) / (1024 * 1024 * 1024)
    
    print(f"\nTotal: {len(csv_files)} files, {total_size:.2f} GB")
    
    # Ask for table name
    table_name = input("\nEnter destination table name: ").strip().upper()
    if not table_name:
        print("ERROR: Table name is required!")
        return
    
    print(f"\nAll CSV files will be loaded into: {SCHEMA}.{table_name}")
    
    # Ask about truncating FIRST (before index rebuild - makes rebuild instant)
    truncate = input("\nTruncate table before loading? (y/n): ").strip().lower()
    
    if truncate == 'y':
        print(f"\nTruncating {SCHEMA}.{table_name}...")
        truncate_table(table_name)
    
    # Ensure all indexes are usable (instant after truncate)
    ensure_indexes_usable(table_name)
    
    # Disable non-unique indexes for faster loading
    disabled_indexes = []
    if DIRECT_PATH:
        print("\nDisabling non-unique indexes for faster loading...")
        disabled_indexes = disable_indexes(table_name)
    
    # Create temp directory for control files and logs
    temp_dir = tempfile.mkdtemp(prefix=f"sqlldr_{table_name}_")
    print(f"Temp directory: {temp_dir}")
    
    print("\n" + "="*80)
    print(f"LOADING {len(csv_files)} FILES IN PARALLEL")
    print(f"Parallel processes: {PARALLEL_FILES}")
    if USE_STAGING_TABLES:
        print("Mode: Staging tables (NO LOCKS)")
    print("="*80)
    
    # Prepare arguments for parallel loading
    args_list = [
        (csv_file, i, table_name, temp_dir)
        for i, csv_file in enumerate(csv_files)
    ]
    
    start_time = datetime.now()
    
    # Load all files in parallel (worker_init ensures Oracle thick mode in each process)
    with Pool(processes=min(PARALLEL_FILES, len(csv_files)), initializer=worker_init) as pool:
        results = pool.map(load_single_file, args_list)
    
    # Clear memory after parallel loading
    gc.collect()
    
    load_elapsed = (datetime.now() - start_time).total_seconds()
    
    # Collect results
    total_rows = sum(r[2] for r in results)
    success_count = sum(1 for r in results if r[1])
    failed_files = [r[3] for r in results if not r[1]]
    staging_tables = [r[4] for r in results if r[1] and r[4]]  # Only successful ones
    
    print(f"\n  Files loaded: {success_count}/{len(csv_files)}")
    print(f"  Rows in staging: {total_rows:,}")
    print(f"  Load time: {load_elapsed:.1f}s")
    
    # Merge staging tables into main table
    if USE_STAGING_TABLES and staging_tables:
        print("\n" + "="*80)
        print(f"MERGING {len(staging_tables)} STAGING TABLES INTO {table_name}")
        print("="*80)
        
        merge_start = datetime.now()
        tables_merged = merge_staging_tables(table_name, staging_tables)
        merge_elapsed = (datetime.now() - merge_start).total_seconds()
        
        print(f"\n  Merged {tables_merged} staging tables")
        print(f"  Merge time: {merge_elapsed:.1f}s ({merge_elapsed/60:.1f} min)")
        
        # Drop staging tables
        drop_staging_tables(staging_tables)
    
    # Rebuild indexes
    if DIRECT_PATH and disabled_indexes:
        print("\n" + "="*80)
        print("REBUILDING INDEXES")
        print("="*80)
        rebuild_indexes(table_name, disabled_indexes)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Cleanup temp directory
    try:
        shutil.rmtree(temp_dir)
        print(f"\nCleaned up temp directory")
    except:
        pass
    
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"Files processed: {success_count}/{len(csv_files)}")
    print(f"Total rows loaded: {total_rows:,}")
    print(f"Load time: {load_elapsed:.1f}s ({load_elapsed/60:.1f} minutes)")
    print(f"Total time (incl index rebuild): {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    if total_rows > 0 and load_elapsed > 0:
        print(f"Load speed: {total_rows/load_elapsed:,.0f} rows/second")
    if failed_files:
        print(f"\nFailed files ({len(failed_files)}):")
        for f in failed_files[:10]:
            print(f"  - {os.path.basename(f)}")
        if len(failed_files) > 10:
            print(f"  ... and {len(failed_files) - 10} more")
    print("="*80)

if __name__ == "__main__":
    main()
