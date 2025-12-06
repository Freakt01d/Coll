import oracledb
import os
import csv
from datetime import datetime
import time
import glob
import gc
import multiprocessing
from multiprocessing import Pool, Manager
import sys

# --- Thick Mode Initialization ---
def init_oracle_client():
    """Initializes the Oracle client for thick mode."""
    try:
        oracledb.init_oracle_client()
    except Exception as e:
        print(f"Error initializing Oracle client: {e}")
        raise

# Destination database connection details
SCHEMA = "placeholder"
dest_username = SCHEMA
dest_password = "placeholder"
dest_hostname = "placeholder.ocp.cloud"
dest_port = "placeholder"
dest_sid = "placeholder"

# Create destination connection string
dest_dsn = oracledb.makedsn(dest_hostname, dest_port, sid=dest_sid)
dest_connection_string = f"{dest_username}/{dest_password}@{dest_dsn}"

# BALANCED PERFORMANCE Configuration
INSERT_BATCH_SIZE = 5000      # Rows per executemany call
COMMIT_INTERVAL = 25000       # Commit every 25K rows
SCAN_INTERVAL = 1             # Check for new files every second
PARALLEL_IMPORTS = 8          # Parallel processes for single file chunking

def format_elapsed_time(seconds):
    """Format elapsed time"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def create_connection():
    """Create optimized destination connection"""
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        # Execute DDL statements first
        cursor.execute("ALTER SESSION SET COMMIT_WRITE = 'BATCH,NOWAIT'")
        cursor.execute("ALTER SESSION DISABLE PARALLEL DML")
        cursor.execute("ALTER SESSION DISABLE PARALLEL QUERY")
        cursor.execute("ALTER SESSION SET RECYCLEBIN = OFF")
        # Optimize date format
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'")
        # Set input sizes AFTER DDL statements
        cursor.setinputsizes(None, INSERT_BATCH_SIZE)
        return connection, cursor
    except Exception as e:
        print(f"Connection error: {e}")
        raise

def truncate_table(table_name):
    """Truncate the destination table"""
    try:
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        print(f"\nTruncating table {table_name}...")
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        cursor.execute(truncate_sql)
        print("Table truncated successfully.")
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"Error truncating table: {e}")
        return False

def count_csv_lines(csv_file):
    """Count total lines in CSV file (excluding header)"""
    with open(csv_file, 'r', encoding='utf-8') as f:
        # Check for header
        first_line = f.readline()
        has_header = first_line.strip().upper().startswith('INDEX_ID')
        
        # Count remaining lines
        count = sum(1 for _ in f)
        if not has_header:
            count += 1  # Include first line if not header
        
        return count, has_header

def import_csv_chunk(args):
    """Import a chunk of CSV for EDS_COMP table"""
    csv_file, table_name, process_id, start_line, end_line, has_header, stats_dict = args
    
    start_time = time.time()
    total_rows = 0
    failed_rows = 0
    
    file_name = os.path.basename(csv_file)
    print(f"[P{process_id}] Starting chunk: lines {start_line:,} to {end_line:,}")
    
    try:
        conn, cursor = create_connection()
        
        # EDS_COMP table columns
        columns = [
            'INDEX_ID',           # VARCHAR2(50) - NOT NULL
            'REF_DATE',           # DATE - NOT NULL
            'NEXT_HISTO_DATE',    # TIMESTAMP(6) - NULL
            'LAST_HISTO_DATE',    # TIMESTAMP(6) - NULL
            'TIME_STAMP',         # TIMESTAMP(6) - NULL
            'CREATED_DATE',       # TIMESTAMP(6) - NOT NULL
            'MODIFIED_DATE'       # TIMESTAMP(6) - NULL
        ]
        
        # Build INSERT with proper date/timestamp conversion
        column_binds = []
        for i, col in enumerate(columns):
            if col == 'REF_DATE':
                # DATE type - use TO_DATE
                column_binds.append(f"TO_DATE(:{i+1}, 'YYYY-MM-DD HH24:MI:SS')")
            elif col in ('NEXT_HISTO_DATE', 'LAST_HISTO_DATE', 'TIME_STAMP', 'CREATED_DATE', 'MODIFIED_DATE'):
                # TIMESTAMP(6) type - use TO_TIMESTAMP
                column_binds.append(f"TO_TIMESTAMP(:{i+1}, 'YYYY-MM-DD HH24:MI:SS.FF6')")
            else:
                # Regular columns (INDEX_ID)
                column_binds.append(f":{i+1}")
        
        insert_sql = f"""
            INSERT /*+ NOPARALLEL */ 
            INTO {table_name} ({', '.join(columns)}) 
            VALUES ({', '.join(column_binds)})
        """
        
        # Read and insert only our chunk
        with open(csv_file, 'r', newline='', encoding='utf-8', buffering=16*1024*1024) as infile:
            reader = csv.reader(infile)
            
            # Skip header if present
            if has_header:
                next(reader, None)
            
            # Skip to our start line
            current_line = 0
            for _ in range(start_line):
                next(reader, None)
                current_line += 1
            
            batch_buffer = []
            commit_counter = 0
            lines_processed = 0
            chunk_size = end_line - start_line
            
            for row_num, row in enumerate(reader, 1):
                # Stop when we've processed our chunk
                if lines_processed >= chunk_size:
                    break
                    
                lines_processed += 1
                
                try:
                    # Convert empty strings to None and handle timestamps
                    converted_row = []
                    for i, val in enumerate(row):
                        if val == '' or val is None:
                            converted_row.append(None)
                        elif i == 0:  # INDEX_ID (VARCHAR2)
                            converted_row.append(val)
                        elif i == 1:  # REF_DATE (DATE)
                            # Ensure date format, add time if missing
                            if val and len(val) == 10:  # Just date YYYY-MM-DD
                                converted_row.append(val + ' 00:00:00')
                            else:
                                converted_row.append(val)
                        elif i in (2, 3, 4, 5, 6):  # TIMESTAMP columns
                            # Ensure timestamp format with microseconds
                            if val and '.' not in val:
                                if len(val) == 10:  # Just date
                                    converted_row.append(val + ' 00:00:00.000000')
                                else:  # Date with time but no microseconds
                                    converted_row.append(val + '.000000')
                            else:
                                converted_row.append(val)
                        else:
                            converted_row.append(val)
                    
                    batch_buffer.append(tuple(converted_row))
                    
                except Exception as e:
                    print(f"[P{process_id}] Row conversion error: {str(e)[:100]}")
                    failed_rows += 1
                    continue
                
                # Process when buffer is full
                if len(batch_buffer) >= INSERT_BATCH_SIZE:
                    try:
                        cursor.executemany(insert_sql, batch_buffer)
                        total_rows += len(batch_buffer)
                        commit_counter += len(batch_buffer)
                        
                        # Commit at intervals
                        if commit_counter >= COMMIT_INTERVAL:
                            conn.commit()
                            commit_counter = 0
                            elapsed = time.time() - start_time
                            speed = total_rows / elapsed if elapsed > 0 else 0
                            print(f"[P{process_id}] {file_name}: {total_rows:,} rows, {speed:,.0f} rows/s")
                            
                            # Update shared stats
                            stats_dict[process_id] = (file_name, total_rows, speed)
                            
                            # GC every 250K rows
                            if total_rows % 250000 < COMMIT_INTERVAL:
                                gc.collect()
                        
                    except Exception as e:
                        error_msg = str(e)
                        print(f"[P{process_id}] Batch insert error: {error_msg[:100]}")
                        
                        # Try row by row on error
                        for row in batch_buffer:
                            try:
                                cursor.execute(insert_sql, row)
                                total_rows += 1
                                commit_counter += 1
                            except:
                                failed_rows += 1
                    
                    batch_buffer = []
                    gc.collect()
            
            # Process remaining rows
            if batch_buffer:
                try:
                    cursor.executemany(insert_sql, batch_buffer)
                    total_rows += len(batch_buffer)
                except Exception as e:
                    print(f"[P{process_id}] Final batch error: {str(e)[:100]}")
                    for row in batch_buffer:
                        try:
                            cursor.execute(insert_sql, row)
                            total_rows += 1
                        except:
                            failed_rows += 1
            
            # Final commit
            conn.commit()
        
        cursor.close()
        conn.close()
        
        gc.collect()
        
        elapsed = time.time() - start_time
        speed = total_rows / elapsed if elapsed > 0 else 0
        
        print(f"[P{process_id}] Completed chunk: {total_rows:,} rows | Failed: {failed_rows} | Time: {format_elapsed_time(elapsed)} | Speed: {speed:,.0f} rows/s")
        
        if process_id in stats_dict:
            del stats_dict[process_id]
        
        return process_id, True, total_rows, failed_rows, elapsed
        
    except Exception as e:
        print(f"[P{process_id}] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return process_id, False, 0, 0, 0

def import_single_csv_chunked(csv_file, table_name):
    """Split a single CSV into 8 chunks and process in parallel"""
    print(f"\n{'='*80}")
    print(f"EDS_COMP CSV IMPORT - {PARALLEL_IMPORTS} PARALLEL CHUNKS")
    print(f"{'='*80}")
    print(f"File: {csv_file}")
    print(f"Table: {table_name}")
    print(f"Chunks: {PARALLEL_IMPORTS}")
    print(f"Batch size: {INSERT_BATCH_SIZE:,} rows")
    print(f"Commit interval: {COMMIT_INTERVAL:,} rows")
    print(f"{'='*80}\n")
    
    # Count total lines
    print("Counting rows in CSV...")
    total_lines, has_header = count_csv_lines(csv_file)
    print(f"Total data rows: {total_lines:,}")
    print(f"Has header: {has_header}\n")
    
    # Calculate chunk sizes
    chunk_size = total_lines // PARALLEL_IMPORTS
    remainder = total_lines % PARALLEL_IMPORTS
    
    # Create chunk ranges
    chunks = []
    current_start = 0
    for i in range(PARALLEL_IMPORTS):
        # Distribute remainder across first few chunks
        extra = 1 if i < remainder else 0
        chunk_end = current_start + chunk_size + extra
        chunks.append((current_start, chunk_end))
        current_start = chunk_end
    
    print("Chunk distribution:")
    for i, (start, end) in enumerate(chunks):
        print(f"  P{i}: lines {start:,} to {end:,} ({end-start:,} rows)")
    print()
    
    start_time = time.time()
    total_rows = 0
    total_failed = 0
    
    manager = Manager()
    stats_dict = manager.dict()
    
    pool = Pool(processes=PARALLEL_IMPORTS, initializer=init_oracle_client)
    
    try:
        # Prepare arguments for all chunks
        import_args = [
            (csv_file, table_name, i, start, end, has_header, stats_dict)
            for i, (start, end) in enumerate(chunks)
        ]
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting {PARALLEL_IMPORTS} parallel chunk imports...")
        
        # Process all chunks in parallel
        results = pool.map(import_csv_chunk, import_args)
        
        # Aggregate results
        all_success = True
        for process_id, success, rows, failed, elapsed in results:
            total_rows += rows
            total_failed += failed
            if not success:
                all_success = False
        
        elapsed_total = time.time() - start_time
        overall_speed = total_rows / elapsed_total if elapsed_total > 0 else 0
        
        print(f"\n{'='*80}")
        print("IMPORT COMPLETE")
        print(f"{'='*80}")
        print(f"Total rows imported: {total_rows:,}")
        print(f"Total rows failed: {total_failed:,}")
        print(f"Total time: {format_elapsed_time(elapsed_total)}")
        print(f"Average speed: {overall_speed:,.0f} rows/second")
        print(f"{'='*80}")
        
        # Delete file after all chunks complete successfully
        if all_success:
            try:
                os.remove(csv_file)
                print(f"\nDeleted: {csv_file}")
            except Exception as e:
                print(f"\nWarning: Could not delete {csv_file}: {e}")
        
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        pool.terminate()
        pool.join()
    finally:
        pool.close()

# Main execution
if __name__ == "__main__":
    print("="*80)
    print("EDS_COMP CSV IMPORTER")
    print(f"{PARALLEL_IMPORTS} Parallel Chunks for Single CSV")
    print("="*80 + "\n")
    
    try:
        init_oracle_client()
    except Exception as e:
        print(f"CRITICAL: Failed to initialize Oracle client - {e}")
        exit(1)
    
    # Table configuration - fully qualified name
    table_name = f'{SCHEMA}.EDS_COMP'
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = input("Enter the path to the CSV file: ").strip()
    
    # Remove quotes if user wrapped path in quotes
    csv_file = csv_file.strip('"').strip("'")
    
    if not os.path.exists(csv_file):
        print(f"ERROR: File '{csv_file}' does not exist!")
        exit(1)
    
    if not csv_file.lower().endswith('.csv'):
        print(f"ERROR: File must be a .csv file! Got: '{csv_file}'")
        exit(1)
    
    truncate = input(f"\nTruncate table {table_name} before starting? (y/n): ").strip().lower()
    if truncate == 'y':
        if not truncate_table(table_name):
            cont = input("Failed to truncate. Continue anyway? (y/n): ").strip().lower()
            if cont != 'y':
                exit(1)
    
    print(f"\nStarting import with {PARALLEL_IMPORTS} parallel chunks...")
    print("CSV will be split into chunks and processed in parallel.\n")
    
    try:
        import_single_csv_chunked(csv_file, table_name)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
