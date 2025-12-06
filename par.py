"""
Fast Partition-by-Partition Data Extractor
- Extracts each partition to a separate CSV file
- Uses parallel processing for speed
- Optimized for 2.5B+ row tables
"""

import os
import sys
import csv
from datetime import datetime
from multiprocessing import Pool, cpu_count
import oracledb

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
# TABLE CONFIGURATION
# =============================================================================
TABLE_NAME = "EDS_IDX_COMP_ITEM"

# Column definitions (from your schema)
COLUMNS = [
    "INDEX_COMPOSITION_ITEM_ID",
    "INDEX_ID",
    "REF_DATE",
    "CURRENCY_ID",
    "MARKET_ID",
    "BO_CODE",
    "BO_MNEMO",
    "UNDERLYING_ID",
    "UNDERLYING_TYPE",
    "WEIGHTING",
    "COMPANY_ID",
    "NB_ISSUED",
    "MARKET_PRODUCT_ID",
    "CREATED_DATE",
    "MODIFIED_DATE"
]

# =============================================================================
# PERFORMANCE SETTINGS (Optimized for 256GB RAM, Xeon Platinum, 10Gbps)
# =============================================================================
FETCH_SIZE = 500000           # Rows to fetch at a time (high RAM = bigger batches)
PARALLEL_EXTRACTS = 32        # Number of partitions to extract in parallel (Xeon = many cores)
WRITE_BUFFER = 64*1024*1024   # 64MB write buffer per file
OUTPUT_DIR = "."              # Output directory for CSV files

# =============================================================================
# CONNECTION SETUP
# =============================================================================
source_dsn = oracledb.makedsn(SOURCE_HOST, SOURCE_PORT, sid=SOURCE_SID)
source_connection_string = f"{SOURCE_USER}/{SOURCE_PASSWORD}@{source_dsn}"

def init_oracle_client():
    """Initialize Oracle client"""
    try:
        oracledb.init_oracle_client()
    except:
        pass

def get_connection():
    """Get a database connection optimized for extraction"""
    connection = oracledb.connect(source_connection_string)
    cursor = connection.cursor()
    # Optimize for bulk reads
    cursor.arraysize = FETCH_SIZE
    cursor.prefetchrows = FETCH_SIZE
    return connection, cursor

def get_partitions(table_name):
    """Get list of partitions for a table"""
    connection, cursor = get_connection()
    
    cursor.execute("""
        SELECT partition_name, num_rows
        FROM all_tab_partitions
        WHERE table_name = :1 AND table_owner = :2
        ORDER BY partition_position
    """, [table_name.upper(), SOURCE_SCHEMA.upper()])
    
    partitions = cursor.fetchall()
    cursor.close()
    connection.close()
    
    return partitions

def format_value(val, col_idx):
    """Format a value for CSV output"""
    if val is None:
        return ""
    
    # REF_DATE (DATE) - index 2
    if col_idx == 2:
        if hasattr(val, 'strftime'):
            return val.strftime('%Y-%m-%d %H:%M:%S')
        return str(val)
    
    # CREATED_DATE, MODIFIED_DATE (TIMESTAMP) - index 13, 14
    if col_idx in (13, 14):
        if hasattr(val, 'strftime'):
            return val.strftime('%Y-%m-%d %H:%M:%S.%f')
        return str(val)
    
    return str(val)

def extract_partition(args):
    """Extract a single partition to CSV"""
    partition_name, partition_num, table_name, output_dir = args
    
    output_file = os.path.join(output_dir, f"{table_name}_{partition_num}.csv")
    
    print(f"  [{partition_num:03d}] Starting: {partition_name} -> {os.path.basename(output_file)}")
    
    start_time = datetime.now()
    row_count = 0
    
    try:
        connection, cursor = get_connection()
        
        # Set date formats for consistent output
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'")
        
        # Query partition directly - fastest method with high parallelism
        sql = f"""
            SELECT /*+ PARALLEL(t, 16) FULL(t) */
                {', '.join(COLUMNS)}
            FROM {SOURCE_SCHEMA}.{table_name} PARTITION ({partition_name}) t
        """
        
        cursor.execute(sql)
        
        # Write to CSV with large buffer (64MB for 10Gbps network)
        with open(output_file, 'w', newline='', encoding='utf-8', buffering=WRITE_BUFFER) as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(COLUMNS)
            
            # Fetch and write in batches
            while True:
                rows = cursor.fetchmany(FETCH_SIZE)
                if not rows:
                    break
                
                # Format and write rows
                for row in rows:
                    formatted_row = [format_value(val, i) for i, val in enumerate(row)]
                    writer.writerow(formatted_row)
                
                row_count += len(rows)
                
                # Progress update every 1M rows
                if row_count % 1000000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = row_count / elapsed if elapsed > 0 else 0
                    print(f"  [{partition_num:03d}] {partition_name}: {row_count:,} rows ({speed:,.0f}/s)")
        
        cursor.close()
        connection.close()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        speed = row_count / elapsed if elapsed > 0 else 0
        file_size = os.path.getsize(output_file) / (1024 * 1024 * 1024)  # GB
        
        print(f"  [{partition_num:03d}] Done: {partition_name} - {row_count:,} rows, {file_size:.2f} GB, {elapsed:.1f}s ({speed:,.0f}/s)")
        
        return partition_name, True, row_count, elapsed, output_file
        
    except Exception as e:
        print(f"  [{partition_num:03d}] ERROR {partition_name}: {e}")
        import traceback
        traceback.print_exc()
        return partition_name, False, 0, 0, None

def main():
    print("="*80)
    print("PARTITION-BY-PARTITION DATA EXTRACTOR")
    print(f"Table: {SOURCE_SCHEMA}.{TABLE_NAME}")
    print("="*80 + "\n")
    
    # Initialize Oracle client
    init_oracle_client()
    
    # Get output directory
    global OUTPUT_DIR
    if len(sys.argv) > 1:
        OUTPUT_DIR = sys.argv[1]
    else:
        OUTPUT_DIR = input("Enter output directory for CSV files: ").strip()
        OUTPUT_DIR = OUTPUT_DIR.strip('"').strip("'")
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")
    
    # Get partitions
    print(f"\nFetching partitions for {TABLE_NAME}...")
    partitions = get_partitions(TABLE_NAME)
    
    if not partitions:
        print("ERROR: No partitions found! Is this a partitioned table?")
        print("\nTrying to extract as non-partitioned table...")
        partitions = [("FULL_TABLE", None)]
    
    total_partitions = len(partitions)
    print(f"Found {total_partitions} partitions total")
    
    # Calculate partition ranges for 4 parts
    part_size = (total_partitions + 3) // 4  # Ceiling division
    
    part_ranges = []
    for i in range(4):
        start_idx = i * part_size
        end_idx = min((i + 1) * part_size, total_partitions)
        if start_idx < total_partitions:
            part_ranges.append((start_idx, end_idx))
    
    # Show parts summary
    print("\n" + "="*80)
    print("PARTITION PARTS")
    print("="*80)
    
    total_rows_estimate = 0
    for part_num, (start_idx, end_idx) in enumerate(part_ranges, 1):
        part_partitions = partitions[start_idx:end_idx]
        part_rows = sum(r or 0 for _, r in part_partitions)
        total_rows_estimate += part_rows
        print(f"\n  Part {part_num}: Partitions {start_idx + 1} to {end_idx} ({end_idx - start_idx} partitions)")
        print(f"          Estimated rows: {part_rows:,}")
        # Show first and last partition names
        if part_partitions:
            print(f"          First: {part_partitions[0][0]}")
            print(f"          Last:  {part_partitions[-1][0]}")
    
    print(f"\n  Total estimated rows: {total_rows_estimate:,}")
    print("="*80)
    
    # Ask which part to extract
    while True:
        part_choice = input(f"\nWhich part do you want to extract? (1-{len(part_ranges)}, or 'q' to quit): ").strip().lower()
        
        if part_choice == 'q':
            print("Aborted.")
            return
        
        try:
            part_num = int(part_choice)
            if 1 <= part_num <= len(part_ranges):
                break
            else:
                print(f"Please enter a number between 1 and {len(part_ranges)}")
        except ValueError:
            print("Invalid input. Enter a number or 'q' to quit.")
    
    # Get selected partitions
    start_idx, end_idx = part_ranges[part_num - 1]
    selected_partitions = partitions[start_idx:end_idx]
    
    print(f"\n" + "="*80)
    print(f"EXTRACTING PART {part_num}: Partitions {start_idx + 1} to {end_idx}")
    print(f"Total partitions in this part: {len(selected_partitions)}")
    print("="*80)
    
    # Show selected partitions
    selected_rows = 0
    for i, (part_name, num_rows) in enumerate(selected_partitions):
        rows_str = f"{num_rows:,}" if num_rows else "unknown"
        selected_rows += num_rows or 0
        if i < 5 or i >= len(selected_partitions) - 2:  # Show first 5 and last 2
            print(f"  {start_idx + i + 1:4d}. {part_name} ({rows_str} rows)")
        elif i == 5:
            print(f"  ... ({len(selected_partitions) - 7} more partitions) ...")
    
    print(f"\nEstimated rows for Part {part_num}: {selected_rows:,}")
    
    # Confirm
    proceed = input(f"\nProceed with extraction of Part {part_num} ({len(selected_partitions)} partitions)? (y/n): ").strip().lower()
    if proceed != 'y':
        print("Aborted.")
        return
    
    # Prepare extraction arguments - use original partition index for file naming
    args_list = [
        (part_name, start_idx + i + 1, TABLE_NAME, OUTPUT_DIR)
        for i, (part_name, _) in enumerate(selected_partitions)
    ]
    
    print("\n" + "="*80)
    print(f"STARTING EXTRACTION - PART {part_num}")
    print(f"Parallel processes: {PARALLEL_EXTRACTS}")
    print("="*80)
    
    start_time = datetime.now()
    
    # Extract partitions in parallel
    with Pool(processes=PARALLEL_EXTRACTS) as pool:
        results = pool.map(extract_partition, args_list)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Summary
    print("\n" + "="*80)
    print(f"PART {part_num} EXTRACTION COMPLETE")
    print("="*80)
    
    total_rows = sum(r[2] for r in results)
    success_count = sum(1 for r in results if r[1])
    failed = [r[0] for r in results if not r[1]]
    
    print(f"Partitions: {success_count}/{len(selected_partitions)} succeeded")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    
    if total_rows > 0 and elapsed > 0:
        print(f"Average speed: {total_rows/elapsed:,.0f} rows/second")
    
    if failed:
        print(f"\nFailed partitions:")
        for p in failed:
            print(f"  - {p}")
    
    # List output files for this part
    print(f"\nOutput files for Part {part_num}:")
    csv_files = sorted([f for f in os.listdir(OUTPUT_DIR) 
                       if f.startswith(f"{TABLE_NAME}_") and f.endswith('.csv')])
    # Filter to just this part's files
    part_files = []
    for f in csv_files:
        try:
            file_num = int(f.replace(f"{TABLE_NAME}_", "").replace(".csv", ""))
            if start_idx + 1 <= file_num <= end_idx:
                part_files.append(f)
        except:
            pass
    
    total_size = 0
    for f in part_files[:10]:
        size = os.path.getsize(os.path.join(OUTPUT_DIR, f)) / (1024*1024*1024)
        total_size += size
        print(f"  {f} ({size:.2f} GB)")
    if len(part_files) > 10:
        print(f"  ... and {len(part_files) - 10} more files")
        for f in part_files[10:]:
            total_size += os.path.getsize(os.path.join(OUTPUT_DIR, f)) / (1024*1024*1024)
    
    print(f"\nPart {part_num} output size: {total_size:.2f} GB")
    print("="*80)
    print(f"\nPart {part_num} done. Run script again to extract another part.")

if __name__ == "__main__":
    main()
