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

# HIGH PERFORMANCE Configuration - 24 processes as requested
INSERT_BATCH_SIZE = 5000      # Rows per executemany call (increased for speed)
COMMIT_INTERVAL = 50000       # Commit every 50K rows
SCAN_INTERVAL = 1             # Check for new files every second
PARALLEL_IMPORTS = 24         # 24 parallel import processes as requested

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
        # Execute DDL statements first (before setinputsizes which primes for bind variables)
        cursor.execute("ALTER SESSION SET COMMIT_WRITE = 'BATCH,NOWAIT'")
        cursor.execute("ALTER SESSION ENABLE PARALLEL DML")
        cursor.execute("ALTER SESSION SET RECYCLEBIN = OFF")
        cursor.execute("ALTER SESSION SET \"_OPTIMIZER_USE_FEEDBACK\" = FALSE")
        # Optimize date format
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'")
        # Set input sizes AFTER DDL statements to avoid DPI-1059 error
        cursor.setinputsizes(None, INSERT_BATCH_SIZE)
        return connection, cursor
    except Exception as e:
        print(f"Connection error: {e}")
        raise

def truncate_table(table_name):
    """Truncate the destination table"""
    try:
        # Create a simple connection without cursor optimizations for DDL
        connection = oracledb.connect(dest_connection_string)
        cursor = connection.cursor()
        print(f"\nTruncating table {table_name}...")
        # Use simple execute for DDL (no bind variables)
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        cursor.execute(truncate_sql)
        print("Table truncated successfully.")
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"Error truncating table: {e}")
        return False

def import_csv_fast(args):
    """Import CSV with proper date/timestamp handling"""
    csv_file, table_name, process_id, stats_dict = args
    
    start_time = time.time()
    total_rows = 0
    failed_rows = 0
    
    file_name = os.path.basename(csv_file)
    print(f"[P{process_id:02d}] Starting: {file_name}")
    
    try:
        conn, cursor = create_connection()
        
        # Column definitions matching your screenshot
        columns = [
            'INDEX_COMPOSITION_ITEM_ID',
            'INDEX_ID', 
            'REF_DATE',                    # DATE type
            'CURRENCY_ID',
            'MARKET_ID',
            'BO_CODE',
            'BO_MNEMO',
            'UNDERLYING_ID',
            'UNDERLYING_TYPE',
            'WEIGHTING',
            'COMPANY_ID',
            'NB_ISSUED',
            'MARKET_PRODUCT_ID',
            'CREATED_DATE',                # TIMESTAMP(6) type
            'MODIFIED_DATE'                # TIMESTAMP(6) type
        ]
        
        # Build INSERT with proper date/timestamp conversion
        column_binds = []
        for i, col in enumerate(columns):
            if col == 'REF_DATE':
                # DATE type - use TO_DATE
                column_binds.append(f"TO_DATE(:{i+1}, 'YYYY-MM-DD HH24:MI:SS')")
            elif col in ('CREATED_DATE', 'MODIFIED_DATE'):
                # TIMESTAMP(6) type - use TO_TIMESTAMP
                column_binds.append(f"TO_TIMESTAMP(:{i+1}, 'YYYY-MM-DD HH24:MI:SS.FF6')")
            else:
                # Regular columns
                column_binds.append(f":{i+1}")
        
        insert_sql = f"""
            INSERT /*+ APPEND_VALUES */ 
            INTO {table_name} ({', '.join(columns)}) 
            VALUES ({', '.join(column_binds)})
        """
        
        # Read and insert in large batches for speed
        with open(csv_file, 'r', newline='', encoding='utf-8', buffering=16*1024*1024) as infile:
            reader = csv.reader(infile)
            
            # Check if first row is header
            first_row = next(reader, None)
            if first_row and first_row[0].upper() == 'INDEX_COMPOSITION_ITEM_ID':
                # It's a header, skip it
                pass
            else:
                # Not a header, process it
                if first_row:
                    reader = [first_row] + list(reader)
            
            batch_buffer = []
            commit_counter = 0
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Convert empty strings to None and handle timestamps
                    converted_row = []
                    for i, val in enumerate(row):
                        if val == '' or val is None:
                            converted_row.append(None)
                        elif i == 2:  # REF_DATE (column 3)
                            # Ensure date format, add time if missing
                            if val and len(val) == 10:  # Just date YYYY-MM-DD
                                converted_row.append(val + ' 00:00:00')
                            else:
                                converted_row.append(val)
                        elif i in (13, 14):  # CREATED_DATE, MODIFIED_DATE
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
                    print(f"[P{process_id:02d}] Row {row_num} conversion error: {str(e)[:100]}")
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
                            print(f"[P{process_id:02d}] {file_name}: {total_rows:,} rows, {speed:,.0f} rows/s")
                            
                            # Update shared stats
                            stats_dict[process_id] = (file_name, total_rows, speed)
                        
                    except Exception as e:
                        error_msg = str(e)
                        if 'ORA-01843' in error_msg:  # Date format error
                            print(f"[P{process_id:02d}] Date format error in batch, retrying with smaller batches...")
                        else:
                            print(f"[P{process_id:02d}] Batch insert error: {error_msg[:100]}")
                        
                        # Try smaller batches on error
                        for i in range(0, len(batch_buffer), 100):
                            small_batch = batch_buffer[i:i+100]
                            try:
                                cursor.executemany(insert_sql, small_batch)
                                total_rows += len(small_batch)
                                commit_counter += len(small_batch)
                            except Exception as e2:
                                # Try row by row as last resort
                                for row in small_batch:
                                    try:
                                        cursor.execute(insert_sql, row)
                                        total_rows += 1
                                        commit_counter += 1
                                    except:
                                        failed_rows += 1
                    
                    batch_buffer = []
                    gc.collect()  # Free memory after large batch
            
            # Process remaining rows
            if batch_buffer:
                try:
                    cursor.executemany(insert_sql, batch_buffer)
                    total_rows += len(batch_buffer)
                except Exception as e:
                    print(f"[P{process_id:02d}] Final batch error: {str(e)[:100]}")
                    # Try row by row for final batch
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
        
        elapsed = time.time() - start_time
        speed = total_rows / elapsed if elapsed > 0 else 0
        
        print(f"[P{process_id:02d}]  Completed: {file_name}")
        print(f"[P{process_id:02d}]   Rows: {total_rows:,} | Failed: {failed_rows} | Time: {format_elapsed_time(elapsed)} | Speed: {speed:,.0f} rows/s")
        
        # Delete the file after successful import
        try:
            os.remove(csv_file)
            print(f"[P{process_id:02d}] Deleted: {file_name}")
        except Exception as e:
            print(f"[P{process_id:02d}] Warning: Could not delete {csv_file}: {e}")
        
        # Clear from shared stats
        if process_id in stats_dict:
            del stats_dict[process_id]
        
        return csv_file, True, total_rows, failed_rows, elapsed
        
    except Exception as e:
        print(f"[P{process_id:02d}] ERROR processing {file_name}: {e}")
        import traceback
        traceback.print_exc()
        return csv_file, False, 0, 0, 0

def show_active_processes(stats_dict):
    """Show currently active import processes"""
    if stats_dict:
        print("\n" + "="*80)
        print("ACTIVE IMPORTS:")
        for pid, (fname, rows, speed) in sorted(stats_dict.items()):
            print(f"  P{pid:02d}: {fname[:40]:40s} | {rows:10,} rows | {speed:8,.0f} rows/s")
        print("="*80)

def monitor_and_import_parallel(watch_dir, table_name):
    """Monitor directory and import CSV files using 24 parallel processes"""
    print(f"\n{'='*80}")
    print("HIGH-PERFORMANCE CSV IMPORT - 24 PARALLEL PROCESSES")
    print(f"{'='*80}")
    print(f"Directory: {watch_dir}")
    print(f"Table: {table_name}")
    print(f"Processes: {PARALLEL_IMPORTS}")
    print(f"Batch size: {INSERT_BATCH_SIZE:,} rows")
    print(f"Commit interval: {COMMIT_INTERVAL:,} rows")
    print(f"\nPress Ctrl+C to stop")
    print(f"{'='*80}\n")
    
    # Check initial CSV count
    initial_files = glob.glob(os.path.join(watch_dir, "*.csv"))
    print(f"Found {len(initial_files)} CSV files to process\n")
    
    processed_files = set()
    total_files = 0
    total_rows = 0
    total_failed = 0
    start_time = time.time()
    
    # Create manager for shared stats
    manager = Manager()
    stats_dict = manager.dict()
    
    # Initialize multiprocessing pool with 24 workers
    pool = Pool(processes=PARALLEL_IMPORTS, initializer=init_oracle_client)
    
    try:
        last_stats_time = time.time()
        
        while True:
            # Find all CSV files in directory
            csv_files = sorted(glob.glob(os.path.join(watch_dir, "*.csv")))
            
            # Get new files to process
            new_files = [f for f in csv_files if f not in processed_files]
            
            if new_files:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Processing {len(new_files)} files...")
                
                # Process all available files in parallel (up to 24 at once)
                batch_size = min(PARALLEL_IMPORTS, len(new_files))
                
                for i in range(0, len(new_files), batch_size):
                    batch_files = new_files[i:i+batch_size]
                    
                    # Prepare arguments for parallel processing
                    import_args = [
                        (f, table_name, idx % PARALLEL_IMPORTS, stats_dict) 
                        for idx, f in enumerate(batch_files, start=i)
                    ]
                    
                    # Process batch in parallel
                    results = pool.map(import_csv_fast, import_args)
                    
                    # Update statistics
                    for csv_file, success, rows, failed, elapsed in results:
                        if success:
                            processed_files.add(csv_file)
                            total_files += 1
                            total_rows += rows
                            total_failed += failed
                
                # Show cumulative stats
                elapsed_total = time.time() - start_time
                overall_speed = total_rows / elapsed_total if elapsed_total > 0 else 0
                
                print(f"\n{'='*80}")
                print(f"CUMULATIVE STATISTICS:")
                print(f"  Files: {total_files:,} processed, {len(csv_files) - len(processed_files):,} remaining")
                print(f"  Rows: {total_rows:,} imported, {total_failed:,} failed")
                print(f"  Speed: {overall_speed:,.0f} rows/second average")
                print(f"  Time: {format_elapsed_time(elapsed_total)}")
                if total_rows > 0:
                    eta_files = len(initial_files) - total_files
                    if eta_files > 0 and total_files > 0:
                        avg_time_per_file = elapsed_total / total_files
                        eta_seconds = avg_time_per_file * eta_files
                        print(f"  ETA: {format_elapsed_time(eta_seconds)} for remaining files")
                print(f"{'='*80}")
            
            # Show active processes periodically
            if time.time() - last_stats_time > 10 and stats_dict:
                show_active_processes(stats_dict)
                last_stats_time = time.time()
            
            # Check if all files are processed
            remaining_files = glob.glob(os.path.join(watch_dir, "*.csv"))
            if not remaining_files and len(processed_files) > 0:
                print("\ All CSV files have been processed and deleted!")
                break
            
            # Wait before next scan
            time.sleep(SCAN_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\nShutting down import processes...")
        pool.terminate()
        pool.join()
    finally:
        pool.close()
        
        print(f"\n\n{'='*80}")
        print("IMPORT COMPLETE")
        print(f"{'='*80}")
        print(f"Total files processed: {total_files:,}")
        print(f"Total rows imported: {total_rows:,}")
        print(f"Total rows failed: {total_failed:,}")
        elapsed_total = time.time() - start_time
        print(f"Total time: {format_elapsed_time(elapsed_total)}")
        if total_rows > 0 and elapsed_total > 0:
            final_speed = total_rows / elapsed_total
            print(f"Average speed: {final_speed:,.0f} rows/second")
            print(f"Throughput: {(total_rows * 200) / (elapsed_total * 1024 * 1024):.1f} MB/s (estimated)")
        print(f"{'='*80}")

# Main execution
if __name__ == "__main__":
    print("="*80)
    print("24-PROCESS PARALLEL CSV IMPORTER")
    print("Optimized for 2.5 billion rows")
    print("="*80 + "\n")
    
    # Initialize Oracle Client
    try:
        init_oracle_client()
    except Exception as e:
        print(f"CRITICAL: Failed to initialize Oracle client - {e}")
        exit(1)
    
    # Table configuration
    table_name = 'EDS_IDX_COMP_ITEM'
    
    # Get directory from command line or prompt
    if len(sys.argv) > 1:
        watch_dir = sys.argv[1]
    else:
        watch_dir = input("Enter the directory path containing CSV files: ").strip()
    
    if not os.path.exists(watch_dir):
        print(f"ERROR: Directory '{watch_dir}' does not exist!")
        exit(1)
    
    # Check for CSV files
    csv_count = len(glob.glob(os.path.join(watch_dir, "*.csv")))
    print(f"\nFound {csv_count} CSV files in {watch_dir}")
    
    if csv_count == 0:
        print("No CSV files found to process!")
        exit(1)
    
    # Ask about truncating table
    if csv_count > 0:
        truncate = input(f"\nTruncate table {table_name} before starting? (y/n): ").strip().lower()
        if truncate == 'y':
            if not truncate_table(table_name):
                cont = input("Failed to truncate. Continue anyway? (y/n): ").strip().lower()
                if cont != 'y':
                    exit(1)
    
    print("\nStarting import with 24 parallel processes...")
    print("This will process all CSV files and delete them after successful import.\n")
    
    try:
        # Start monitoring and importing with 24 parallel processes
        monitor_and_import_parallel(watch_dir, table_name)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
