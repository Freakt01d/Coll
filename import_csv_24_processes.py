import oracledb
import os
import time
import glob
import gc
import multiprocessing
from multiprocessing import Pool, Manager
import sys
from datetime import datetime

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

# PERFORMANCE Configuration
COMMIT_INTERVAL = 5000        # Commit every N statements
SCAN_INTERVAL = 1             # Check for new files every second
PARALLEL_IMPORTS = 24         # Parallel processes

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
        return connection, cursor
    except Exception as e:
        print(f"Connection error: {e}")
        raise

def execute_sql_file(args):
    """Execute SQL statements from a .sql file"""
    sql_file, process_id, stats_dict = args
    
    start_time = time.time()
    total_statements = 0
    failed_statements = 0
    
    file_name = os.path.basename(sql_file)
    print(f"[P{process_id:02d}] Starting: {file_name}")
    
    try:
        conn, cursor = create_connection()
        commit_counter = 0
        
        # Read the SQL file
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split by semicolon to get individual statements
        # Handle statements that may span multiple lines
        statements = []
        current_statement = []
        
        for line in content.split('\n'):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('--') or line.startswith('/*'):
                continue
            
            current_statement.append(line)
            
            # Check if statement ends with semicolon
            if line.endswith(';'):
                full_statement = ' '.join(current_statement)
                # Remove trailing semicolon for Oracle execution
                full_statement = full_statement.rstrip(';').strip()
                if full_statement:
                    statements.append(full_statement)
                current_statement = []
        
        # Don't forget last statement if no trailing semicolon
        if current_statement:
            full_statement = ' '.join(current_statement).rstrip(';').strip()
            if full_statement:
                statements.append(full_statement)
        
        print(f"[P{process_id:02d}] Found {len(statements)} statements in {file_name}")
        
        # Execute statements
        for i, stmt in enumerate(statements):
            try:
                # Only execute INSERT, UPDATE, DELETE, MERGE statements
                stmt_upper = stmt.upper().strip()
                if stmt_upper.startswith(('INSERT', 'UPDATE', 'DELETE', 'MERGE')):
                    cursor.execute(stmt)
                    total_statements += 1
                    commit_counter += 1
                    
                    # Commit at intervals
                    if commit_counter >= COMMIT_INTERVAL:
                        conn.commit()
                        commit_counter = 0
                        elapsed = time.time() - start_time
                        speed = total_statements / elapsed if elapsed > 0 else 0
                        print(f"[P{process_id:02d}] {file_name}: {total_statements:,} stmts, {speed:,.0f} stmts/s")
                        
                        # Update shared stats
                        stats_dict[process_id] = (file_name, total_statements, speed)
                        
                        # Periodic GC
                        if total_statements % 50000 == 0:
                            gc.collect()
                
            except Exception as e:
                failed_statements += 1
                if failed_statements <= 5:  # Only print first 5 errors
                    print(f"[P{process_id:02d}] Error on statement {i+1}: {str(e)[:100]}")
        
        # Final commit
        conn.commit()
        
        cursor.close()
        conn.close()
        
        # Force garbage collection
        gc.collect()
        
        elapsed = time.time() - start_time
        speed = total_statements / elapsed if elapsed > 0 else 0
        
        print(f"[P{process_id:02d}] Completed: {file_name}")
        print(f"[P{process_id:02d}]   Statements: {total_statements:,} | Failed: {failed_statements} | Time: {format_elapsed_time(elapsed)} | Speed: {speed:,.0f} stmts/s")
        
        # Delete the file after successful import
        try:
            os.remove(sql_file)
            print(f"[P{process_id:02d}] Deleted: {file_name}")
        except Exception as e:
            print(f"[P{process_id:02d}] Warning: Could not delete {sql_file}: {e}")
        
        # Clear from shared stats
        if process_id in stats_dict:
            del stats_dict[process_id]
        
        return sql_file, True, total_statements, failed_statements, elapsed
        
    except Exception as e:
        print(f"[P{process_id:02d}] ERROR processing {file_name}: {e}")
        import traceback
        traceback.print_exc()
        return sql_file, False, 0, 0, 0

def process_sql_files_parallel(watch_dir):
    """Process SQL files using parallel processes"""
    print(f"\n{'='*80}")
    print("PARALLEL SQL FILE EXECUTOR")
    print(f"{'='*80}")
    print(f"Directory: {watch_dir}")
    print(f"Processes: {PARALLEL_IMPORTS}")
    print(f"Commit interval: {COMMIT_INTERVAL} statements")
    print(f"\nPress Ctrl+C to stop")
    print(f"{'='*80}\n")
    
    # Check initial SQL count
    initial_files = glob.glob(os.path.join(watch_dir, "*.sql"))
    print(f"Found {len(initial_files)} SQL files to process\n")
    
    processed_files = set()
    total_files = 0
    total_statements = 0
    total_failed = 0
    start_time = time.time()
    
    # Create manager for shared stats
    manager = Manager()
    stats_dict = manager.dict()
    
    # Initialize multiprocessing pool
    pool = Pool(processes=PARALLEL_IMPORTS, initializer=init_oracle_client)
    
    try:
        while True:
            # Find all SQL files in directory
            sql_files = sorted(glob.glob(os.path.join(watch_dir, "*.sql")))
            
            # Get new files to process
            new_files = [f for f in sql_files if f not in processed_files]
            
            if new_files:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Processing {len(new_files)} files...")
                
                # Process in batches
                batch_size = min(PARALLEL_IMPORTS, len(new_files))
                
                for i in range(0, len(new_files), batch_size):
                    batch_files = new_files[i:i+batch_size]
                    
                    # Prepare arguments for parallel processing
                    import_args = [
                        (f, idx % PARALLEL_IMPORTS, stats_dict) 
                        for idx, f in enumerate(batch_files, start=i)
                    ]
                    
                    # Process batch in parallel
                    results = pool.map(execute_sql_file, import_args)
                    
                    # Update statistics
                    for sql_file, success, stmts, failed, elapsed in results:
                        if success:
                            processed_files.add(sql_file)
                            total_files += 1
                            total_statements += stmts
                            total_failed += failed
                
                # Show cumulative stats
                elapsed_total = time.time() - start_time
                overall_speed = total_statements / elapsed_total if elapsed_total > 0 else 0
                
                print(f"\n{'='*80}")
                print(f"CUMULATIVE STATISTICS:")
                print(f"  Files: {total_files:,} processed")
                print(f"  Statements: {total_statements:,} executed, {total_failed:,} failed")
                print(f"  Speed: {overall_speed:,.0f} statements/second average")
                print(f"  Time: {format_elapsed_time(elapsed_total)}")
                print(f"{'='*80}")
            
            # Check if all files are processed
            remaining_files = glob.glob(os.path.join(watch_dir, "*.sql"))
            if not remaining_files and len(processed_files) > 0:
                print("\nAll SQL files have been processed and deleted!")
                break
            
            # Wait before next scan
            time.sleep(SCAN_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        pool.terminate()
        pool.join()
    finally:
        pool.close()
        
        print(f"\n\n{'='*80}")
        print("EXECUTION COMPLETE")
        print(f"{'='*80}")
        print(f"Total files processed: {total_files:,}")
        print(f"Total statements executed: {total_statements:,}")
        print(f"Total statements failed: {total_failed:,}")
        elapsed_total = time.time() - start_time
        print(f"Total time: {format_elapsed_time(elapsed_total)}")
        if total_statements > 0 and elapsed_total > 0:
            final_speed = total_statements / elapsed_total
            print(f"Average speed: {final_speed:,.0f} statements/second")
        print(f"{'='*80}")

# Main execution
if __name__ == "__main__":
    print("="*80)
    print("24-PROCESS PARALLEL SQL EXECUTOR")
    print("Execute INSERT statements from .sql files")
    print("="*80 + "\n")
    
    # Initialize Oracle Client
    try:
        init_oracle_client()
    except Exception as e:
        print(f"CRITICAL: Failed to initialize Oracle client - {e}")
        exit(1)
    
    # Get directory from command line or prompt
    if len(sys.argv) > 1:
        watch_dir = sys.argv[1]
    else:
        watch_dir = input("Enter the directory path containing SQL files: ").strip()
    
    if not os.path.exists(watch_dir):
        print(f"ERROR: Directory '{watch_dir}' does not exist!")
        exit(1)
    
    # Check for SQL files
    sql_count = len(glob.glob(os.path.join(watch_dir, "*.sql")))
    print(f"\nFound {sql_count} SQL files in {watch_dir}")
    
    if sql_count == 0:
        print("No SQL files found to process!")
        exit(1)
    
    print("\nStarting execution with 24 parallel processes...")
    print("This will process all SQL files and delete them after successful execution.\n")
    
    try:
        process_sql_files_parallel(watch_dir)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
