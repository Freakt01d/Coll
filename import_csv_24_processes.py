import os
import re
import subprocess
import glob
import sys
from datetime import datetime

# Database connection details
SCHEMA = "placeholder"
DB_USER = SCHEMA
DB_PASSWORD = "placeholder"
DB_HOST = "placeholder.ocp.cloud"
DB_PORT = "placeholder"
DB_SID = "placeholder"

# SQL*Loader connection string
DB_CONNECT = f"{DB_USER}/{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_SID}"

# Performance settings
ROWS_PER_COMMIT = 50000       # Commit every N rows
DIRECT_PATH = True            # Use direct path loading (faster)
PARALLEL = True               # Enable parallel loading

def parse_insert_to_values(insert_sql):
    """Extract values from an INSERT statement"""
    # Match: INSERT INTO table (...) VALUES (...)
    # or: INSERT INTO table VALUES (...)
    match = re.search(r'VALUES\s*\((.*)\)\s*;?\s*$', insert_sql, re.IGNORECASE | re.DOTALL)
    if match:
        values_str = match.group(1)
        return parse_values(values_str)
    return None

def parse_values(values_str):
    """Parse VALUES clause into list of values"""
    values = []
    current_value = ""
    in_string = False
    string_char = None
    paren_depth = 0
    
    i = 0
    while i < len(values_str):
        char = values_str[i]
        
        if not in_string:
            if char in ("'", '"'):
                in_string = True
                string_char = char
                current_value += char
            elif char == '(':
                paren_depth += 1
                current_value += char
            elif char == ')':
                paren_depth -= 1
                current_value += char
            elif char == ',' and paren_depth == 0:
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char
        else:
            current_value += char
            if char == string_char:
                # Check for escaped quote
                if i + 1 < len(values_str) and values_str[i + 1] == string_char:
                    i += 1
                    current_value += string_char
                else:
                    in_string = False
        i += 1
    
    if current_value.strip():
        values.append(current_value.strip())
    
    return values

def clean_value(val):
    """Clean a value for CSV output"""
    val = val.strip()
    
    # Handle NULL
    if val.upper() == 'NULL':
        return ''
    
    # Handle TO_DATE/TO_TIMESTAMP functions - extract the date string
    date_match = re.match(r"TO_(?:DATE|TIMESTAMP)\s*\(\s*'([^']+)'", val, re.IGNORECASE)
    if date_match:
        return date_match.group(1)
    
    # Remove surrounding quotes
    if (val.startswith("'") and val.endswith("'")) or \
       (val.startswith('"') and val.endswith('"')):
        val = val[1:-1]
        # Unescape doubled quotes
        val = val.replace("''", "'").replace('""', '"')
    
    return val

def convert_sql_to_csv(sql_file, output_dir):
    """Convert SQL INSERT file to CSV"""
    base_name = os.path.basename(sql_file)
    # Extract table name
    if base_name.lower().endswith('_inserts.sql'):
        table_name = base_name[:-12]
    else:
        table_name = base_name[:-4]
    
    csv_file = os.path.join(output_dir, f"{table_name}.csv")
    
    print(f"Converting {base_name} -> {table_name}.csv")
    
    row_count = 0
    columns = None
    
    with open(sql_file, 'r', encoding='utf-8', errors='replace') as infile:
        with open(csv_file, 'w', encoding='utf-8', newline='') as outfile:
            current_statement = ""
            
            for line in infile:
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('--'):
                    continue
                
                current_statement += " " + line
                
                # Check if statement is complete (ends with semicolon)
                if line.endswith(';'):
                    # Parse the INSERT statement
                    if 'INSERT' in current_statement.upper():
                        values = parse_insert_to_values(current_statement)
                        if values:
                            # Extract column names from first INSERT if present
                            if columns is None:
                                col_match = re.search(r'INSERT\s+INTO\s+\S+\s*\(([^)]+)\)', 
                                                     current_statement, re.IGNORECASE)
                                if col_match:
                                    columns = [c.strip() for c in col_match.group(1).split(',')]
                            
                            # Clean and write values
                            cleaned = [clean_value(v) for v in values]
                            # Escape commas and quotes in values
                            csv_values = []
                            for v in cleaned:
                                if ',' in v or '"' in v or '\n' in v:
                                    v = '"' + v.replace('"', '""') + '"'
                                csv_values.append(v)
                            
                            outfile.write(','.join(csv_values) + '\n')
                            row_count += 1
                            
                            if row_count % 100000 == 0:
                                print(f"  Converted {row_count:,} rows...")
                    
                    current_statement = ""
    
    print(f"  Done: {row_count:,} rows -> {csv_file}")
    return table_name, csv_file, row_count, columns

def create_control_file(table_name, csv_file, columns, output_dir):
    """Create SQL*Loader control file"""
    ctl_file = os.path.join(output_dir, f"{table_name}.ctl")
    
    # Build column list
    if columns:
        col_spec = ',\n    '.join(columns)
    else:
        col_spec = "-- columns auto-detected"
    
    control_content = f"""-- SQL*Loader control file for {table_name}
-- Generated by sqlldr_import.py

LOAD DATA
INFILE '{csv_file}'
BADFILE '{output_dir}/{table_name}.bad'
DISCARDFILE '{output_dir}/{table_name}.dsc'
{"APPEND" if not DIRECT_PATH else "APPEND"}
INTO TABLE {SCHEMA}.{table_name}
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
    {col_spec}
)
"""
    
    with open(ctl_file, 'w') as f:
        f.write(control_content)
    
    print(f"  Created control file: {ctl_file}")
    return ctl_file

def run_sqlldr(table_name, ctl_file, csv_file, output_dir):
    """Run SQL*Loader for a table"""
    log_file = os.path.join(output_dir, f"{table_name}.log")
    
    cmd = [
        'sqlldr',
        f'userid={DB_CONNECT}',
        f'control={ctl_file}',
        f'log={log_file}',
        f'rows={ROWS_PER_COMMIT}',
        'errors=1000000',  # Allow many errors before stopping
        'bindsize=20000000',  # 20MB bind buffer
        'readsize=20000000',  # 20MB read buffer
    ]
    
    if DIRECT_PATH:
        cmd.append('direct=true')
    if PARALLEL:
        cmd.append('parallel=true')
    
    print(f"\nRunning SQL*Loader for {table_name}...")
    print(f"  Command: {' '.join(cmd[:3])}...")
    
    start_time = datetime.now()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)  # 2 hour timeout
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if result.returncode == 0:
            print(f"  SUCCESS: {table_name} loaded in {elapsed:.1f}s")
        elif result.returncode == 2:
            print(f"  WARNING: {table_name} loaded with warnings (check {log_file})")
        else:
            print(f"  ERROR: {table_name} failed (return code {result.returncode})")
            print(f"  Check log: {log_file}")
            if result.stderr:
                print(f"  stderr: {result.stderr[:500]}")
        
        return result.returncode == 0 or result.returncode == 2
        
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT: {table_name} exceeded 2 hour limit")
        return False
    except FileNotFoundError:
        print("  ERROR: sqlldr not found. Make sure Oracle Client is in PATH.")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False

def truncate_table(table_name):
    """Truncate table using sqlplus"""
    full_table = f"{SCHEMA}.{table_name}"
    print(f"Truncating {full_table}...")
    
    sql_cmd = f"TRUNCATE TABLE {full_table};"
    
    try:
        # Use sqlplus for DDL
        cmd = ['sqlplus', '-S', DB_CONNECT]
        result = subprocess.run(cmd, input=sql_cmd, capture_output=True, text=True, timeout=60)
        
        if 'ORA-' in result.stdout or 'ORA-' in result.stderr:
            print(f"  Error: {result.stdout} {result.stderr}")
            return False
        
        print(f"  Truncated {full_table}")
        return True
    except Exception as e:
        print(f"  Error truncating: {e}")
        return False

def main():
    print("="*80)
    print("SQL*LOADER BULK IMPORT")
    print("Convert SQL INSERT files to CSV and load via SQL*Loader")
    print("="*80 + "\n")
    
    # Get directory
    if len(sys.argv) > 1:
        sql_dir = sys.argv[1]
    else:
        sql_dir = input("Enter directory containing SQL INSERT files: ").strip()
        sql_dir = sql_dir.strip('"').strip("'")
    
    if not os.path.exists(sql_dir):
        print(f"ERROR: Directory '{sql_dir}' does not exist!")
        return
    
    # Find SQL files
    sql_files = glob.glob(os.path.join(sql_dir, "*.sql"))
    
    if not sql_files:
        print("No SQL files found!")
        return
    
    print(f"Found {len(sql_files)} SQL files:\n")
    for f in sorted(sql_files):
        size_mb = os.path.getsize(f) / (1024 * 1024)
        print(f"  - {os.path.basename(f)} ({size_mb:.1f} MB)")
    
    # Create output directory for CSV and control files
    output_dir = os.path.join(sql_dir, "sqlldr_data")
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nOutput directory: {output_dir}")
    
    # Ask about truncating
    truncate = input("\nTruncate tables before loading? (y/n): ").strip().lower()
    
    print("\n" + "="*80)
    print("STEP 1: Converting SQL to CSV")
    print("="*80)
    
    tables = []
    for sql_file in sorted(sql_files):
        table_name, csv_file, row_count, columns = convert_sql_to_csv(sql_file, output_dir)
        ctl_file = create_control_file(table_name, csv_file, columns, output_dir)
        tables.append((table_name, csv_file, ctl_file, row_count))
    
    print("\n" + "="*80)
    print("STEP 2: Loading data via SQL*Loader")
    print("="*80)
    
    if truncate == 'y':
        print("\nTruncating tables...")
        for table_name, _, _, _ in tables:
            truncate_table(table_name)
    
    success_count = 0
    total_rows = 0
    start_time = datetime.now()
    
    for table_name, csv_file, ctl_file, row_count in tables:
        if run_sqlldr(table_name, ctl_file, csv_file, output_dir):
            success_count += 1
            total_rows += row_count
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Tables loaded: {success_count}/{len(tables)}")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {elapsed:.1f}s")
    if total_rows > 0 and elapsed > 0:
        print(f"Speed: {total_rows/elapsed:,.0f} rows/second")
    print("="*80)

if __name__ == "__main__":
    main()
