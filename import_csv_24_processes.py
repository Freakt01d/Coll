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

# Performance settings
PARALLEL_CHUNKS = 16          # Split each CSV into this many chunks
ROWS_PER_COMMIT = 100000      # Commit every N rows
DIRECT_PATH = True            # Fast direct path - indexes handled separately
STAGGER_DELAY = 2.0           # Seconds delay between starting each loader (avoids lock contention)

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

def count_lines(filepath):
    """Count lines in a file efficiently"""
    count = 0
    with open(filepath, 'rb') as f:
        for _ in f:
            count += 1
    return count

def get_csv_header(csv_file):
    """Get header row from CSV"""
    with open(csv_file, 'r', encoding='utf-8') as f:
        first_line = f.readline().strip()
        # Check if it looks like a header (no pure numbers, has text)
        parts = first_line.split(',')
        if parts and not parts[0].replace('.','').replace('-','').isdigit():
            return parts
    return None

def split_csv_file(csv_file, num_chunks, output_dir):
    """Split CSV file into chunks, preserving header"""
    print(f"  Splitting into {num_chunks} chunks...")
    
    # Count total lines
    total_lines = count_lines(csv_file)
    
    # Check for header
    header = None
    with open(csv_file, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        parts = first_line.strip().split(',')
        # Simple heuristic: if first field isn't a number, it's a header
        if parts and not parts[0].replace('.','').replace('-','').replace(' ','').isdigit():
            header = first_line
            total_lines -= 1  # Don't count header
    
    if total_lines == 0:
        print("  ERROR: Empty file!")
        return []
    
    lines_per_chunk = (total_lines + num_chunks - 1) // num_chunks
    
    base_name = os.path.splitext(os.path.basename(csv_file))[0]
    chunk_files = []
    
    with open(csv_file, 'r', encoding='utf-8', buffering=16*1024*1024) as infile:
        # Skip header if present
        if header:
            next(infile)
        
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
                print(f"    Chunk {chunk_num}: {current_lines:,} lines")
                current_file = None
                current_lines = 0
                chunk_num += 1
        
        if current_file:
            current_file.close()
            print(f"    Chunk {chunk_num}: {current_lines:,} lines")
    
    print(f"  Split into {len(chunk_files)} chunks")
    return chunk_files, header

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

def run_sqlldr_chunk(args):
    """Run SQL*Loader for a single chunk"""
    chunk_file, ctl_file, table_name, chunk_id, output_dir = args
    
    # Stagger start to avoid lock contention (ORA-00054)
    time.sleep(chunk_id * STAGGER_DELAY)
    
    base = os.path.splitext(os.path.basename(chunk_file))[0]
    log_file = os.path.join(output_dir, f"{base}.log")
    
    cmd = [
        'sqlldr',
        f'userid={DB_CONNECT}',
        f'control={ctl_file}',
        f'log={log_file}',
        f'rows={ROWS_PER_COMMIT}',
        'errors=100000',
        'bindsize=20000000',
        'readsize=20000000',
        'silent=(header,feedback)',
    ]
    
    if DIRECT_PATH:
        cmd.append('direct=true')
    
    start_time = datetime.now()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=14400)  # 4 hour timeout
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
        
        success = result.returncode in (0, 2)  # 0=success, 2=warnings
        
        if success:
            speed = rows_loaded / elapsed if elapsed > 0 else 0
            print(f"    [Chunk {chunk_id}] {rows_loaded:,} rows in {elapsed:.1f}s ({speed:,.0f}/s)")
        else:
            print(f"    [Chunk {chunk_id}] FAILED - check {log_file}")
        
        return chunk_id, success, rows_loaded, elapsed
        
    except subprocess.TimeoutExpired:
        print(f"    [Chunk {chunk_id}] TIMEOUT")
        return chunk_id, False, 0, 0
    except Exception as e:
        print(f"    [Chunk {chunk_id}] ERROR: {e}")
        return chunk_id, False, 0, 0

# Create connection string for oracledb
dest_dsn = oracledb.makedsn(DB_HOST, DB_PORT, sid=DB_SID)
dest_connection_string = f"{DB_USER}/{DB_PASSWORD}@{dest_dsn}"

def init_oracle_client():
    """Initialize Oracle client for thick mode"""
    try:
        oracledb.init_oracle_client()
    except Exception as e:
        print(f"Oracle client init: {e}")

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

def process_csv_file(csv_file, table_name, truncate_first=False, is_first_file=True):
    """Process a single CSV file with parallel SQL*Loader"""
    file_name = os.path.basename(csv_file)
    file_size = os.path.getsize(csv_file) / (1024 * 1024 * 1024)  # GB
    
    print(f"\n{'='*80}")
    print(f"Processing: {file_name} ({file_size:.2f} GB)")
    print(f"Table: {SCHEMA}.{table_name}")
    print(f"{'='*80}")
    
    # Create temp directory for chunks
    temp_dir = tempfile.mkdtemp(prefix=f"sqlldr_{table_name}_")
    print(f"Temp directory: {temp_dir}")
    
    disabled_indexes = []
    
    try:
        # For first file of each table: truncate and disable indexes
        if is_first_file:
            if truncate_first:
                truncate_table(table_name)
            # Disable indexes for direct path loading
            if DIRECT_PATH:
                disabled_indexes = disable_indexes(table_name)
        
        # Get header/columns
        header_parts = get_csv_header(csv_file)
        
        # Split the file
        chunk_files, header = split_csv_file(csv_file, PARALLEL_CHUNKS, temp_dir)
        
        if not chunk_files:
            print("  No chunks created!")
            return False, 0
        
        # Create control files for each chunk
        print(f"  Creating control files...")
        ctl_files = []
        for chunk_file in chunk_files:
            ctl = create_control_file(table_name, chunk_file, header_parts, temp_dir)
            ctl_files.append(ctl)
        
        # Prepare arguments for parallel execution
        args_list = [
            (chunk_files[i], ctl_files[i], table_name, i, temp_dir)
            for i in range(len(chunk_files))
        ]
        
        # Run SQL*Loader in parallel
        print(f"\n  Loading {len(chunk_files)} chunks in parallel...")
        start_time = datetime.now()
        
        with Pool(processes=PARALLEL_CHUNKS) as pool:
            results = pool.map(run_sqlldr_chunk, args_list)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # Aggregate results
        total_rows = sum(r[2] for r in results)
        success_count = sum(1 for r in results if r[1])
        
        print(f"\n  RESULTS for {file_name}:")
        print(f"    Chunks: {success_count}/{len(chunk_files)} succeeded")
        print(f"    Rows loaded: {total_rows:,}")
        print(f"    Time: {elapsed:.1f}s")
        if total_rows > 0 and elapsed > 0:
            print(f"    Speed: {total_rows/elapsed:,.0f} rows/second")
        
        success = success_count == len(chunk_files)
        
        # Cleanup
        if success:
            print(f"  Cleaning up temp files...")
            shutil.rmtree(temp_dir)
            
            # Delete original CSV
            try:
                os.remove(csv_file)
                print(f"  Deleted: {file_name}")
            except Exception as e:
                print(f"  Warning: Could not delete {file_name}: {e}")
        else:
            print(f"  Keeping temp files for debugging: {temp_dir}")
        
        gc.collect()
        return success, total_rows, table_name, disabled_indexes
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, table_name, disabled_indexes

def main():
    print("="*80)
    print("SQL*LOADER PARALLEL CSV IMPORT")
    print(f"Splits each CSV into {PARALLEL_CHUNKS} chunks for parallel loading")
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
    
    # Exclude chunk files from previous runs (contain "_chunk" in name)
    csv_files = [f for f in csv_files if '_chunk' not in os.path.basename(f).lower()]
    csv_files = sorted(csv_files)
    
    if not csv_files:
        print("No CSV files found!")
        return
    
    print(f"Found {len(csv_files)} CSV files:\n")
    total_size = 0
    for f in csv_files:
        size_gb = os.path.getsize(f) / (1024 * 1024 * 1024)
        total_size += size_gb
        print(f"  - {os.path.basename(f)} ({size_gb:.2f} GB)")
    
    # Ask for table name
    table_name = input("\nEnter destination table name: ").strip().upper()
    if not table_name:
        print("ERROR: Table name is required!")
        return
    
    print(f"\nAll CSV files will be loaded into: {SCHEMA}.{table_name}")
    print(f"\nTotal size: {total_size:.2f} GB")
    
    # Ask about truncating FIRST (before index rebuild - makes rebuild instant)
    truncate = input("\nTruncate table before loading? (y/n): ").strip().lower()
    
    if truncate == 'y':
        print(f"\nTruncating {SCHEMA}.{table_name}...")
        truncate_table(table_name)
    
    # Ensure all indexes are usable (instant after truncate)
    ensure_indexes_usable(table_name)
    
    print("\n" + "="*80)
    print("STARTING IMPORT")
    print("="*80)
    
    # Track disabled indexes
    disabled_indexes = []
    
    total_files = 0
    total_rows = 0
    failed_files = []
    start_time = datetime.now()
    
    for i, csv_file in enumerate(csv_files):
        is_first = (i == 0)  # Only first file triggers truncate/index disable
        
        success, rows, tbl_name, disabled_idx = process_csv_file(
            csv_file,
            table_name,
            truncate_first=False,  # Already truncated above
            is_first_file=is_first
        )
        
        if is_first and disabled_idx:
            disabled_indexes = disabled_idx
        
        if success:
            total_files += 1
            total_rows += rows
        else:
            failed_files.append(csv_file)
    
    load_elapsed = (datetime.now() - start_time).total_seconds()
    
    # Rebuild indexes
    if DIRECT_PATH and disabled_indexes:
        print("\n" + "="*80)
        print("REBUILDING INDEXES")
        print("="*80)
        rebuild_indexes(table_name, disabled_indexes)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"Files processed: {total_files}/{len(csv_files)}")
    print(f"Total rows loaded: {total_rows:,}")
    print(f"Load time: {load_elapsed:.1f}s ({load_elapsed/60:.1f} minutes)")
    print(f"Total time (incl index rebuild): {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    if total_rows > 0 and load_elapsed > 0:
        print(f"Load speed: {total_rows/load_elapsed:,.0f} rows/second")
    if failed_files:
        print(f"\nFailed files:")
        for f in failed_files:
            print(f"  - {f}")
    print("="*80)

if __name__ == "__main__":
    main()
