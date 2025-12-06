"""
Database to Database Loader using SQL*Loader
- Extracts data from source Oracle database
- Exports to CSV with headers
- Loads into destination Oracle database using SQL*Loader
"""

import os
import sys
import csv
import tempfile
import shutil
import subprocess
from datetime import datetime
from multiprocessing import Pool
import oracledb
import gc

# =============================================================================
# SOURCE DATABASE CONFIGURATION
# =============================================================================
SOURCE_SCHEMA = "placeholder"
SOURCE_USER = SOURCE_SCHEMA
SOURCE_PASSWORD = "placeholder"
SOURCE_HOST = "placeholder"
SOURCE_PORT = "placeholder"
SOURCE_SID = "placeholder"

# =============================================================================
# DESTINATION DATABASE CONFIGURATION
# =============================================================================
DEST_SCHEMA = "placeholder"
DEST_USER = DEST_SCHEMA
DEST_PASSWORD = "placeholder"
DEST_HOST = "placeholder"
DEST_PORT = "placeholder"
DEST_SID = "placeholder"

# =============================================================================
# PERFORMANCE SETTINGS
# =============================================================================
FETCH_BATCH_SIZE = 50000      # Rows to fetch at a time from source
PARALLEL_CHUNKS = 16          # Split CSV into this many chunks
SQLLDR_ROWS = 100000          # SQL*Loader commit interval
DIRECT_PATH = True            # Use direct path loading (faster)
STAGGER_DELAY = 0.5           # Seconds delay between starting each loader (avoids lock contention)

# =============================================================================
# TABLES TO TRANSFER (add your tables here)
# =============================================================================
TABLES_TO_TRANSFER = [
    # "TABLE_NAME_1",
    # "TABLE_NAME_2",
    # Add more tables as needed
]

# =============================================================================
# CONNECTION SETUP
# =============================================================================
source_dsn = oracledb.makedsn(SOURCE_HOST, SOURCE_PORT, sid=SOURCE_SID)
source_connection_string = f"{SOURCE_USER}/{SOURCE_PASSWORD}@{source_dsn}"

dest_dsn = oracledb.makedsn(DEST_HOST, DEST_PORT, sid=DEST_SID)
dest_connection_string = f"{DEST_USER}/{DEST_PASSWORD}@{dest_dsn}"

# SQL*Loader connection string
SQLLDR_CONNECT = f"{DEST_USER}/{DEST_PASSWORD}@{DEST_HOST}:{DEST_PORT}/{DEST_SID}"


def init_oracle_client():
    """Initialize Oracle client for thick mode"""
    try:
        oracledb.init_oracle_client()
    except Exception as e:
        print(f"Oracle client init: {e}")


def get_source_connection():
    """Get connection to source database"""
    return oracledb.connect(source_connection_string)


def get_dest_connection():
    """Get connection to destination database"""
    return oracledb.connect(dest_connection_string)


def get_table_columns(table_name, connection_string, schema):
    """Get column names and types from a table"""
    try:
        connection = oracledb.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT column_name, data_type, data_length, data_precision, data_scale
            FROM all_tab_columns 
            WHERE table_name = :1 AND owner = :2
            ORDER BY column_id
        """, [table_name.upper(), schema.upper()])
        columns = cursor.fetchall()
        cursor.close()
        connection.close()
        return columns
    except Exception as e:
        print(f"  Error getting columns: {e}")
        return []


def get_row_count(table_name, connection):
    """Get approximate row count from table"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except:
        return 0


def format_elapsed(seconds):
    """Format elapsed time"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


# =============================================================================
# STEP 1: EXTRACT DATA FROM SOURCE
# =============================================================================
def extract_to_csv(table_name, output_dir):
    """Extract table data from source database to CSV"""
    print(f"\n  Extracting {table_name} from source...")
    
    csv_file = os.path.join(output_dir, f"{table_name}.csv")
    
    try:
        connection = get_source_connection()
        cursor = connection.cursor()
        
        # Get columns
        columns = get_table_columns(table_name, source_connection_string, SOURCE_SCHEMA)
        if not columns:
            print(f"    ERROR: No columns found for {table_name}")
            return None, 0, []
        
        column_names = [col[0] for col in columns]
        column_types = {col[0]: col[1] for col in columns}
        
        print(f"    Columns: {len(column_names)}")
        
        # Get row count
        row_count_est = get_row_count(table_name, connection)
        print(f"    Estimated rows: {row_count_est:,}")
        
        # Build SELECT with proper date formatting
        select_cols = []
        for col_name in column_names:
            col_type = column_types.get(col_name, '')
            if 'TIMESTAMP' in col_type:
                select_cols.append(f"TO_CHAR({col_name}, 'YYYY-MM-DD HH24:MI:SS.FF6') AS {col_name}")
            elif col_type == 'DATE':
                select_cols.append(f"TO_CHAR({col_name}, 'YYYY-MM-DD HH24:MI:SS') AS {col_name}")
            else:
                select_cols.append(col_name)
        
        select_sql = f"SELECT {', '.join(select_cols)} FROM {SOURCE_SCHEMA}.{table_name}"
        
        # Execute and fetch
        cursor.arraysize = FETCH_BATCH_SIZE
        cursor.execute(select_sql)
        
        row_count = 0
        start_time = datetime.now()
        
        with open(csv_file, 'w', encoding='utf-8', newline='', buffering=16*1024*1024) as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            
            # Write header
            writer.writerow(column_names)
            
            # Fetch and write in batches
            while True:
                rows = cursor.fetchmany(FETCH_BATCH_SIZE)
                if not rows:
                    break
                
                for row in rows:
                    # Convert None to empty string
                    writer.writerow(['' if v is None else v for v in row])
                
                row_count += len(rows)
                
                if row_count % 500000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = row_count / elapsed if elapsed > 0 else 0
                    pct = (row_count / row_count_est * 100) if row_count_est > 0 else 0
                    print(f"    Extracted {row_count:,} rows ({pct:.1f}%) - {speed:,.0f} rows/s")
        
        cursor.close()
        connection.close()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        speed = row_count / elapsed if elapsed > 0 else 0
        file_size = os.path.getsize(csv_file) / (1024 * 1024)
        
        print(f"    Done: {row_count:,} rows in {format_elapsed(elapsed)} ({speed:,.0f} rows/s)")
        print(f"    File: {csv_file} ({file_size:.1f} MB)")
        
        return csv_file, row_count, columns
        
    except Exception as e:
        print(f"    ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None, 0, []


# =============================================================================
# STEP 2: PREPARE DESTINATION
# =============================================================================
def truncate_dest_table(table_name):
    """Truncate destination table"""
    try:
        connection = get_dest_connection()
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {DEST_SCHEMA}.{table_name}")
        cursor.close()
        connection.close()
        print(f"    Truncated {DEST_SCHEMA}.{table_name}")
        return True
    except Exception as e:
        print(f"    Error truncating: {e}")
        return False


def get_table_indexes(table_name):
    """Get indexes on destination table"""
    try:
        connection = get_dest_connection()
        cursor = connection.cursor()
        cursor.execute("""
            SELECT index_name, uniqueness 
            FROM all_indexes 
            WHERE table_name = :1 AND owner = :2
            AND index_type != 'LOB'
        """, [table_name.upper(), DEST_SCHEMA.upper()])
        indexes = cursor.fetchall()
        cursor.close()
        connection.close()
        return indexes
    except Exception as e:
        print(f"    Error getting indexes: {e}")
        return []


def disable_indexes(table_name):
    """Disable indexes for faster loading"""
    indexes = get_table_indexes(table_name)
    if not indexes:
        return []
    
    print(f"    Disabling {len(indexes)} indexes...")
    disabled = []
    
    try:
        connection = get_dest_connection()
        cursor = connection.cursor()
        
        for idx_name, uniqueness in indexes:
            try:
                cursor.execute(f"ALTER INDEX {DEST_SCHEMA}.{idx_name} UNUSABLE")
                disabled.append((idx_name, uniqueness))
            except Exception as e:
                print(f"      Warning: {idx_name}: {e}")
        
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"    Error: {e}")
    
    return disabled


def rebuild_indexes(table_name, indexes):
    """Rebuild indexes after loading"""
    if not indexes:
        return
    
    print(f"    Rebuilding {len(indexes)} indexes...")
    
    try:
        connection = get_dest_connection()
        cursor = connection.cursor()
        
        for idx_name, uniqueness in indexes:
            try:
                cursor.execute(f"ALTER INDEX {DEST_SCHEMA}.{idx_name} REBUILD PARALLEL")
                cursor.execute(f"ALTER INDEX {DEST_SCHEMA}.{idx_name} NOPARALLEL")
                print(f"      Rebuilt: {idx_name}")
            except Exception as e:
                print(f"      Warning: {idx_name}: {e}")
        
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"    Error: {e}")


# =============================================================================
# STEP 3: LOAD DATA VIA SQL*LOADER
# =============================================================================
def count_csv_lines(csv_file):
    """Count lines in CSV (excluding header)"""
    count = 0
    with open(csv_file, 'rb') as f:
        for _ in f:
            count += 1
    return count - 1  # Exclude header


def split_csv(csv_file, num_chunks, output_dir):
    """Split CSV into chunks for parallel loading"""
    total_lines = count_csv_lines(csv_file)
    lines_per_chunk = (total_lines + num_chunks - 1) // num_chunks
    
    base_name = os.path.splitext(os.path.basename(csv_file))[0]
    chunk_files = []
    
    with open(csv_file, 'r', encoding='utf-8', buffering=16*1024*1024) as infile:
        header = next(infile)  # Read header
        
        chunk_num = 0
        current_lines = 0
        current_file = None
        
        for line in infile:
            if current_file is None:
                chunk_path = os.path.join(output_dir, f"{base_name}_chunk{chunk_num:02d}.csv")
                current_file = open(chunk_path, 'w', encoding='utf-8', buffering=16*1024*1024)
                chunk_files.append(chunk_path)
            
            current_file.write(line)
            current_lines += 1
            
            if current_lines >= lines_per_chunk:
                current_file.close()
                current_file = None
                current_lines = 0
                chunk_num += 1
        
        if current_file:
            current_file.close()
    
    return chunk_files, header.strip().split(',')


def create_control_file(table_name, csv_file, columns, column_types, output_dir):
    """Create SQL*Loader control file"""
    base = os.path.splitext(os.path.basename(csv_file))[0]
    ctl_file = os.path.join(output_dir, f"{base}.ctl")
    
    # Build column specs with date formatting
    col_specs = []
    for col_name in columns:
        col_type = column_types.get(col_name.upper(), '')
        
        if 'TIMESTAMP' in col_type:
            col_specs.append(f'{col_name} TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF6"')
        elif col_type == 'DATE':
            col_specs.append(f'{col_name} DATE "YYYY-MM-DD HH24:MI:SS"')
        else:
            col_specs.append(col_name)
    
    col_spec = ',\n    '.join(col_specs)
    
    control_content = f"""LOAD DATA
INFILE '{csv_file}'
BADFILE '{output_dir}/{base}.bad'
DISCARDFILE '{output_dir}/{base}.dsc'
APPEND
INTO TABLE {DEST_SCHEMA}.{table_name}
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
    {col_spec}
)
"""
    
    with open(ctl_file, 'w') as f:
        f.write(control_content)
    
    return ctl_file


def run_sqlldr(args):
    """Run SQL*Loader for a chunk"""
    chunk_file, ctl_file, table_name, chunk_id, output_dir = args
    
    # Stagger start to avoid lock contention (ORA-00054)
    import time
    time.sleep(chunk_id * STAGGER_DELAY)
    
    base = os.path.splitext(os.path.basename(chunk_file))[0]
    log_file = os.path.join(output_dir, f"{base}.log")
    
    cmd = [
        'sqlldr',
        f'userid={SQLLDR_CONNECT}',
        f'control={ctl_file}',
        f'log={log_file}',
        f'rows={SQLLDR_ROWS}',
        'errors=100000',
        'bindsize=20000000',
        'readsize=20000000',
        'silent=(header,feedback)',
    ]
    
    if DIRECT_PATH:
        cmd.append('direct=true')
    
    start_time = datetime.now()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=14400)
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # Parse log for row count
        rows_loaded = 0
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                import re
                content = f.read()
                match = re.search(r'(\d+) Rows? successfully loaded', content)
                if match:
                    rows_loaded = int(match.group(1))
        
        success = result.returncode in (0, 2)
        
        if success:
            speed = rows_loaded / elapsed if elapsed > 0 else 0
            print(f"      [Chunk {chunk_id:02d}] {rows_loaded:,} rows in {elapsed:.1f}s ({speed:,.0f}/s)")
        else:
            print(f"      [Chunk {chunk_id:02d}] FAILED - check {log_file}")
        
        return success, rows_loaded
        
    except subprocess.TimeoutExpired:
        print(f"      [Chunk {chunk_id:02d}] TIMEOUT")
        return False, 0
    except Exception as e:
        print(f"      [Chunk {chunk_id:02d}] ERROR: {e}")
        return False, 0


def load_csv_parallel(table_name, csv_file, columns, column_types, work_dir):
    """Load CSV into destination using parallel SQL*Loader"""
    print(f"\n  Loading {table_name} to destination...")
    
    # Split CSV into chunks
    print(f"    Splitting into {PARALLEL_CHUNKS} chunks...")
    chunk_files, header_cols = split_csv(csv_file, PARALLEL_CHUNKS, work_dir)
    print(f"    Created {len(chunk_files)} chunks")
    
    # Create control files
    ctl_files = []
    for chunk_file in chunk_files:
        ctl = create_control_file(table_name, chunk_file, header_cols, column_types, work_dir)
        ctl_files.append(ctl)
    
    # Prepare arguments
    args_list = [
        (chunk_files[i], ctl_files[i], table_name, i, work_dir)
        for i in range(len(chunk_files))
    ]
    
    # Run in parallel
    print(f"    Loading {len(chunk_files)} chunks in parallel...")
    start_time = datetime.now()
    
    with Pool(processes=PARALLEL_CHUNKS) as pool:
        results = pool.map(run_sqlldr, args_list)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Aggregate results
    total_rows = sum(r[1] for r in results)
    success_count = sum(1 for r in results if r[0])
    
    speed = total_rows / elapsed if elapsed > 0 else 0
    
    print(f"    Done: {total_rows:,} rows in {format_elapsed(elapsed)} ({speed:,.0f} rows/s)")
    print(f"    Chunks: {success_count}/{len(chunk_files)} succeeded")
    
    return success_count == len(chunk_files), total_rows


# =============================================================================
# MAIN TRANSFER FUNCTION
# =============================================================================
def transfer_table(table_name, work_dir, truncate=True):
    """Transfer a single table from source to destination"""
    print(f"\n{'='*80}")
    print(f"TRANSFERRING: {table_name}")
    print(f"{'='*80}")
    
    table_start = datetime.now()
    
    # Create table-specific work directory
    table_dir = os.path.join(work_dir, table_name)
    os.makedirs(table_dir, exist_ok=True)
    
    # Step 1: Extract from source
    csv_file, row_count, columns = extract_to_csv(table_name, table_dir)
    if not csv_file or row_count == 0:
        print(f"  FAILED: No data extracted")
        return False, 0
    
    column_types = {col[0]: col[1] for col in columns}
    column_names = [col[0] for col in columns]
    
    # Step 2: Prepare destination
    if truncate:
        truncate_dest_table(table_name)
    
    disabled_indexes = []
    if DIRECT_PATH:
        disabled_indexes = disable_indexes(table_name)
    
    # Step 3: Load to destination
    success, loaded_rows = load_csv_parallel(
        table_name, csv_file, column_names, column_types, table_dir
    )
    
    # Step 4: Rebuild indexes
    if disabled_indexes:
        rebuild_indexes(table_name, disabled_indexes)
    
    # Cleanup
    if success:
        shutil.rmtree(table_dir)
        print(f"  Cleaned up temp files")
    
    elapsed = (datetime.now() - table_start).total_seconds()
    print(f"\n  TABLE COMPLETE: {table_name}")
    print(f"  Rows: {loaded_rows:,} | Time: {format_elapsed(elapsed)}")
    
    gc.collect()
    return success, loaded_rows


def main():
    print("="*80)
    print("DATABASE TO DATABASE LOADER")
    print("Extract from Source -> CSV -> SQL*Loader -> Destination")
    print("="*80)
    
    # Initialize Oracle client
    init_oracle_client()
    
    # Validate tables list
    if not TABLES_TO_TRANSFER:
        print("\nERROR: No tables specified in TABLES_TO_TRANSFER list!")
        print("Edit the script and add table names to transfer.")
        return
    
    print(f"\nSource: {SOURCE_USER}@{SOURCE_HOST}/{SOURCE_SID}")
    print(f"Destination: {DEST_USER}@{DEST_HOST}/{DEST_SID}")
    print(f"\nTables to transfer: {len(TABLES_TO_TRANSFER)}")
    for t in TABLES_TO_TRANSFER:
        print(f"  - {t}")
    
    # Test connections
    print("\nTesting connections...")
    try:
        conn = get_source_connection()
        conn.close()
        print("  Source: OK")
    except Exception as e:
        print(f"  Source: FAILED - {e}")
        return
    
    try:
        conn = get_dest_connection()
        conn.close()
        print("  Destination: OK")
    except Exception as e:
        print(f"  Destination: FAILED - {e}")
        return
    
    # Confirm
    truncate = input("\nTruncate destination tables before loading? (y/n): ").strip().lower()
    proceed = input("Start transfer? (y/n): ").strip().lower()
    
    if proceed != 'y':
        print("Cancelled.")
        return
    
    # Create work directory
    work_dir = tempfile.mkdtemp(prefix="db_transfer_")
    print(f"\nWork directory: {work_dir}")
    
    # Transfer tables
    print("\n" + "="*80)
    print("STARTING TRANSFER")
    print("="*80)
    
    start_time = datetime.now()
    results = []
    
    for table_name in TABLES_TO_TRANSFER:
        success, rows = transfer_table(table_name, work_dir, truncate=(truncate == 'y'))
        results.append((table_name, success, rows))
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Summary
    print("\n" + "="*80)
    print("TRANSFER COMPLETE")
    print("="*80)
    
    total_rows = 0
    success_count = 0
    
    for table_name, success, rows in results:
        status = "OK" if success else "FAILED"
        print(f"  {table_name}: {rows:,} rows - {status}")
        if success:
            success_count += 1
            total_rows += rows
    
    print(f"\nTables: {success_count}/{len(TABLES_TO_TRANSFER)} succeeded")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {format_elapsed(elapsed)}")
    if total_rows > 0 and elapsed > 0:
        print(f"Overall speed: {total_rows/elapsed:,.0f} rows/second")
    print("="*80)
    
    # Cleanup work directory
    try:
        shutil.rmtree(work_dir)
        print(f"\nCleaned up: {work_dir}")
    except:
        print(f"\nNote: Could not clean up {work_dir}")


if __name__ == "__main__":
    main()
