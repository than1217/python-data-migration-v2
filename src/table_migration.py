import os
import subprocess
import re
import json
import time
import sys
import getpass
import argparse
import mysql.connector
import logging
from mysql.connector import Error
import config
from tqdm import tqdm

# Set up logging to migration.log
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_file = os.path.join(base_dir, "migration.log")
state_file = os.path.join(base_dir, "migration_state.json")

MAX_RETRIES = 3
RETRY_DELAY = 5

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a' # Use 'a' to append so we keep history across sessions
)
logger = logging.getLogger(__name__)

def load_state():
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading state file: {e}")
    return {"processed_tables": [], "migrated_tables": [], "pattern": None, "from_list": None}

def save_state(state):
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        logger.error(f"Error saving state file: {e}")

def get_lib_tables(pattern=None, from_list=None):
    """Connects to MySQL and retrieves tables based on a pattern or a specific list."""
    logger.info(f"Connecting to MySQL Server {config.DB_HOST}...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=config.DB_HOST,
                database=config.DB_DATABASE,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                connect_timeout=10
            )
            if conn.is_connected():
                logger.info(f"Successfully connected to database '{config.DB_DATABASE}'.")
                
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                
                all_tables = [row[0] for row in cursor.fetchall()]
                tables = []
                
                if from_list:
                    # Filter based on the explicit list provided
                    tables = [t for t in all_tables if t in from_list]
                else:
                    # Filter based on pattern (default to '^lib_.*' if none provided)
                    if not pattern:
                        pattern = r'^lib_.*'
                        
                    regex = re.compile(pattern)
                    
                    for table_name in all_tables:
                        if not regex.match(table_name):
                            continue
                            
                        # 1. Exclude tables ending with _old
                        if table_name.endswith('_old'):
                            continue
                        
                        # 2. Exclude tables with year values below 2020
                        # Find all 4-digit numbers in the table name
                        years_found = re.findall(r'\d{4}', table_name)
                        is_old_year = False
                        for year_str in years_found:
                            year = int(year_str)
                            # If it's a realistic year value and below 2020, exclude it
                            if 2000 <= year < 2020:
                                is_old_year = True
                                break
                        
                        if is_old_year:
                            logger.info(f"Skipping table '{table_name}' (reason: year below 2020)")
                            continue
                        
                        tables.append(table_name)
                
                cursor.close()
                conn.close()
                return tables
                
        except Error as e:
            logger.error(f"Attempt {attempt}/{MAX_RETRIES} - Error connecting to MySQL: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    
    return []

def run_mysqldump(table, output_file):
    """Executes mysqldump on the command line for the provided table"""
    
    # Added explicit connection closure parameters where applicable to mysqldump
    # --skip-opt prevents some default options that might hold locks/connections
    command_str = (
        f'"{config.MYSQLDUMP_PATH}" --protocol=TCP -h {config.DB_HOST} -u {config.DB_USER} --password="{config.DB_PASSWORD}" '
        f'--no-tablespaces --skip-lock-tables --skip-add-locks --set-gtid-purged=OFF --single-transaction --quick --max_allowed_packet=1G {config.DB_DATABASE} {table}'
    )
    
    logger.info(f"Dumping table '{table}' to {output_file}...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                result = subprocess.run(command_str, stdout=f, stderr=subprocess.PIPE, text=True, check=True, shell=True)
            
            # Add a small delay to avoid too many connections on the source server
            time.sleep(0.5)
            
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Attempt {attempt}/{MAX_RETRIES} failed to dump table '{table}': {e.stderr}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Attempt {attempt}/{MAX_RETRIES} unexpected error during mysqldump for '{table}': {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

def process_dump_file(input_file, output_file, table_name, suffix):
    """Reads the raw SQL dump, updates the table name, engine, and collation, and saves it to a new file"""
    logger.info(f"Processing table '{table_name}' dump line by line to handle large files...")
    try:
        new_table_name = f"{table_name}{suffix}"
        table_name_pattern = re.compile(rf"`{table_name}`")

        # 2. Update Table-level options (ENGINE, CHARSET, COLLATE, ROW_FORMAT)
        def update_table_options(match):
            options = match.group(1)
            # Ensure ENGINE is InnoDB
            options = re.sub(r'ENGINE\s*=\s*\w+', 'ENGINE=InnoDB', options, flags=re.IGNORECASE)
            
            # Ensure CHARSET is utf8mb4
            if re.search(r'(?:DEFAULT\s+)?CHARSET\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'(?:DEFAULT\s+)?CHARSET\s*=\s*\w+', 'DEFAULT CHARSET=utf8mb4', options, flags=re.IGNORECASE)
            else:
                options += ' DEFAULT CHARSET=utf8mb4'
                
            # Ensure COLLATE is utf8mb4_0900_ai_ci
            if re.search(r'COLLATE\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'COLLATE\s*=\s*\w+', 'COLLATE=utf8mb4_0900_ai_ci', options, flags=re.IGNORECASE)
            else:
                options += ' COLLATE=utf8mb4_0900_ai_ci'
                
            # Ensure ROW_FORMAT is DYNAMIC
            if re.search(r'ROW_FORMAT\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'ROW_FORMAT\s*=\s*\w+', 'ROW_FORMAT=DYNAMIC', options, flags=re.IGNORECASE)
            else:
                options += ' ROW_FORMAT=DYNAMIC'
                
            return f") {options};"

        table_options_pattern = re.compile(r'\)\s*(ENGINE\s*=[^;]+);', flags=re.IGNORECASE)
            
        # 3. Replace Column-level character sets and collations
        charset_pattern = re.compile(r'CHARACTER SET\s+\w+', flags=re.IGNORECASE)
        collate_pattern = re.compile(r'COLLATE\s+\w+', flags=re.IGNORECASE)
        
        def inject_charset_collate(match):
            col_def = match.group(0)
            if re.search(r'CHARACTER SET', col_def, flags=re.IGNORECASE):
                return col_def
            return match.group(1) + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci" + match.group(2)

        column_pattern = re.compile(r'(`[^`]+`\s+(?:varchar\([^)]+\)|char\([^)]+\)|enum\([^)]+\)|text|longtext|mediumtext|tinytext))([^,\n]*)', flags=re.IGNORECASE)
        
        with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                # 1. Replace the table name with the suffixed table name.
                if f"`{table_name}`" in line:
                    line = table_name_pattern.sub(f"`{new_table_name}`", line)
                
                # Skip heavy regex for INSERT statements which form the bulk of data in huge tables
                if not line.startswith('INSERT INTO'):
                    if line.lstrip().startswith(')') and 'ENGINE=' in line.upper():
                        line = table_options_pattern.sub(update_table_options, line)
                    elif line.lstrip().startswith('`'):
                        line = charset_pattern.sub('CHARACTER SET utf8mb4', line)
                        line = collate_pattern.sub('COLLATE utf8mb4_0900_ai_ci', line)
                        line = column_pattern.sub(inject_charset_collate, line)

                f_out.write(line)
        logger.info(f"Successfully processed table '{table_name}'.")
            
    except Exception as e:
        logger.error(f"Error processing dump file for '{table_name}': {e}")

def create_destination_db():
    """Connects to MySQL and creates the destination database if it doesn't exist."""
    logger.info(f"Connecting to destination server {config.DEST_DB_HOST}...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=config.DEST_DB_HOST,
                user=config.DEST_DB_USER,
                password=config.DEST_DB_PASSWORD,
                connect_timeout=10
            )
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.DEST_DB_DATABASE}")
                logger.info(f"Database '{config.DEST_DB_DATABASE}' ensured on destination.")
                cursor.close()
                conn.close()
                return True
        except Error as e:
            logger.error(f"Attempt {attempt}/{MAX_RETRIES} - Error creating destination DB: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

def load_sql_file(filepath):
    """Loads a SQL dump file into the destination database using the mysql client."""
    filename = os.path.basename(filepath)
    # Added --connect_timeout=10 to explicitly manage connection timeouts and avoid hanging connections.
    # Added -e "source <file>" to run the script and immediately exit, ensuring the connection drops.
    command_str = (
        f'"{config.MYSQL_PATH}" --protocol=TCP --connect_timeout=10 --max_allowed_packet=1G -h {config.DEST_DB_HOST} -u {config.DEST_DB_USER} --password="{config.DEST_DB_PASSWORD}" '
        f'{config.DEST_DB_DATABASE} -e "source {filepath}"'
    )

    logger.info(f"Loading {filename} into {config.DEST_DB_DATABASE}...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Add explicit close/kill for the process after run to ensure clean disconnect
            result = subprocess.run(command_str, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, check=True, shell=True)
            logger.info(f"Successfully loaded {filename}")
            
            # Add a small delay to avoid overwhelming the MySQL server with too many rapid connections
            time.sleep(1)
            
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Attempt {attempt}/{MAX_RETRIES} failed to load {filename}: {e.stderr}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Attempt {attempt}/{MAX_RETRIES} unexpected error during SQL loading of {filename}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

def run_migration(tables, state, suffix):
    # Determine the folder name (e.g. 'v2', 'v3', or a custom name like 'v4') from the suffix.
    folder_name = suffix.strip('_') if suffix else 'v2'

    # Go up one directory from 'src' to the root directory, then into 'output'
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, "output", "raw", folder_name)
    processed_dir = os.path.join(base_dir, "output", "processed", folder_name)
    
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    if not tables:
        print("No tables found to migrate. See migration.log for details.")
        return
        
    print(f"\nMigrating {len(tables)} tables (logging details to migration.log)...")
    start_time = time.time()
    
    processed_files = []
    # Step 1 & 2: Dump and Process
    for table in tqdm(tables, desc="Dumping and Processing", unit="table", leave=False):
        raw_dump = os.path.join(raw_dir, f"{table}_raw{suffix}.sql")
        processed_dump = os.path.join(processed_dir, f"{table}{suffix}.sql")
        
        if table in state["processed_tables"]:
            logger.info(f"Skipping dump/process for '{table}', already completed in previous session.")
            processed_files.append((table, processed_dump))
            continue
        
        if run_mysqldump(table, raw_dump):
            process_dump_file(raw_dump, processed_dump, table, suffix)
            processed_files.append((table, processed_dump))
            state["processed_tables"].append(table)
            save_state(state)
        else:
            logger.warning(f"Skipping processing for table '{table}' due to dump failure.")
            
    # Step 3: Load into destination
    if create_destination_db() and processed_files:
        # Count items that were already migrated previously
        successful_migrations = sum(1 for table, _ in processed_files if table in state["migrated_tables"])
        
        for table, f in tqdm(processed_files, desc="Migrating to Destination", unit="file", leave=False):
            if table in state["migrated_tables"]:
                logger.info(f"Skipping migration for '{table}', already loaded in previous session.")
                continue
                
            if load_sql_file(f):
                successful_migrations += 1
                state["migrated_tables"].append(table)
                save_state(state)
        
        summary = f"Migration complete: {successful_migrations}/{len(tables)} tables migrated."
        print(f"\n{summary}")
        logger.info(summary)
        
        # Reset state after a completely successful run where all expected tables migrated
        if successful_migrations == len(tables):
            logger.info("All tables migrated successfully. Resetting migration state.")
            save_state({"processed_tables": [], "migrated_tables": [], "pattern": None, "from_list": None})
            
    else:
        logger.error("Migration skipped or failed to connect to destination.")
        print("\nMigration failed. See migration.log for details.")

    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    h, m = divmod(m, 60)
    time_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
    print(f"Total migration time: {time_str}")
    logger.info(f"Total migration time: {time_str}")

    logger.info("--- MIGRATION FINISHED ---")

def choose_database():
    """Connects to MySQL to list available databases and prompts user to select one."""
    print(f"\nConnecting to Server {config.DB_HOST} to fetch databases...")
    try:
        conn = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            connect_timeout=10
        )
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Attempt to increase max_connections (Requires SUPER privilege)
            try:
                cursor.execute("SET GLOBAL max_connections = 300")
                logger.info("Successfully increased max_connections to 300.")
            except Error as e:
                logger.warning(f"Could not set max_connections (normal if lacking privileges): {e}")

            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall() if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
            cursor.close()
            conn.close()
            
            if not databases:
                print("No user databases found on this server.")
                return False
                
            print("\nAvailable Databases:")
            for i, db in enumerate(databases, 1):
                print(f"{i}. {db}")
                
            while True:
                choice = input("\nSelect a database number: ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(databases):
                    selected_db = databases[int(choice) - 1]
                    config.DB_DATABASE = selected_db
                    print(f"Selected Database: {config.DB_DATABASE}")
                    return True
                else:
                    print("Invalid choice. Please try again.")
    except Error as e:
        print(f"\nAuthentication or connection failed: {e}")
        return False

def choose_destination_database():
    """Connects to MySQL destination server to list available databases and prompts user to select one."""
    print(f"\nConnecting to Destination Server {config.DEST_DB_HOST} to fetch databases...")
    try:
        conn = mysql.connector.connect(
            host=config.DEST_DB_HOST,
            user=config.DEST_DB_USER,
            password=config.DEST_DB_PASSWORD,
            connect_timeout=10
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall() if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
            cursor.close()
            conn.close()
            
            if not databases:
                print("No user databases found on destination server.")
                
            print("\nAvailable Destination Databases:")
            for i, db in enumerate(databases, 1):
                print(f"{i}. {db}")
            print("0. Create a new database")
                
            while True:
                choice = input("\nSelect a destination database number or 0 to create new: ").strip()
                if choice == '0':
                    new_db = input("Enter new destination database name: ").strip()
                    if new_db:
                        config.DEST_DB_DATABASE = new_db
                        print(f"Selected Destination Database: {config.DEST_DB_DATABASE}")
                        return True
                    else:
                        print("Database name cannot be empty.")
                elif choice.isdigit() and 1 <= int(choice) <= len(databases):
                    selected_db = databases[int(choice) - 1]
                    config.DEST_DB_DATABASE = selected_db
                    print(f"Selected Destination Database: {config.DEST_DB_DATABASE}")
                    return True
                else:
                    print("Invalid choice. Please try again.")
    except Error as e:
        print(f"\nAuthentication or connection failed for destination server: {e}")
        manual_db = input("Enter destination database name manually: ").strip()
        if manual_db:
            config.DEST_DB_DATABASE = manual_db
            print(f"Selected Destination Database: {config.DEST_DB_DATABASE}")
            return True
        return False

def run_headless(config_file):
    """Runs the migration non-interactively using a JSON configuration file."""
    print(f"\n=============================================")
    print(f"      STARTING HEADLESS MIGRATION JOB        ")
    print(f"=============================================")
    print(f"Loading configuration from: {config_file}")

    if not os.path.exists(config_file):
        logger.error(f"Configuration file '{config_file}' not found.")
        print(f"Configuration file '{config_file}' not found.")
        return

    try:
        with open(config_file, 'r') as f:
            cfg = json.load(f)
    except Exception as e:
        logger.error(f"Error loading configuration file: {e}")
        print(f"Error loading configuration file: {e}")
        return

    # Extract source config
    config.DB_HOST = cfg.get('db_host', config.DB_HOST)
    if config.DB_HOST == 'localhost':
        config.DB_HOST = '127.0.0.1'
    config.DB_USER = cfg.get('db_user', config.DB_USER)
    config.DB_PASSWORD = cfg.get('db_password', config.DB_PASSWORD)
    config.DB_DATABASE = cfg.get('db_database', config.DB_DATABASE)

    # Extract destination config
    config.DEST_DB_HOST = cfg.get('dest_db_host', config.DEST_DB_HOST)
    if config.DEST_DB_HOST == 'localhost':
        config.DEST_DB_HOST = '127.0.0.1'
    config.DEST_DB_USER = cfg.get('dest_db_user', config.DEST_DB_USER)
    config.DEST_DB_PASSWORD = cfg.get('dest_db_password', config.DEST_DB_PASSWORD)
    config.DEST_DB_DATABASE = cfg.get('dest_db_database', config.DEST_DB_DATABASE)

    suffix = cfg.get('suffix', '')
    pattern = cfg.get('pattern')
    table_list_str = cfg.get('table_list')
    table_list = None
    if table_list_str:
        if isinstance(table_list_str, str):
            table_list = [t.strip() for t in table_list_str.split(',') if t.strip()]
        elif isinstance(table_list_str, list):
            table_list = table_list_str

    if not pattern and not table_list:
        logger.error("Configuration file must specify 'pattern' or 'table_list'.")
        print("Configuration file must specify 'pattern' or 'table_list'.")
        return

    # Optional: read whether to clear previous state or resume
    resume = cfg.get('resume', True)
    
    print(f"\n[Configuration Loaded]")
    print(f"Source DB: {config.DB_HOST} -> {config.DB_DATABASE}")
    print(f"Dest DB:   {config.DEST_DB_HOST} -> {config.DEST_DB_DATABASE}")
    print(f"Suffix:    '{suffix}'")
    
    if resume:
        print(f"Resume Mode: Enabled (will skip already processed tables)")
        state = load_state()
    else:
        print(f"Resume Mode: Disabled (starting fresh)")
        state = {"processed_tables": [], "migrated_tables": []}

    current_state = {
        "processed_tables": state.get("processed_tables", []), 
        "migrated_tables": state.get("migrated_tables", []), 
        "pattern": pattern, 
        "from_list": table_list, 
        "suffix": suffix,
        "db_host": config.DB_HOST,
        "db_user": config.DB_USER,
        "db_database": config.DB_DATABASE,
        "dest_db_host": config.DEST_DB_HOST,
        "dest_db_user": config.DEST_DB_USER,
        "dest_db_database": config.DEST_DB_DATABASE
    }
    save_state(current_state)
    
    print(f"\n[Fetching Tables]")
    if table_list:
        print(f"Using explicit table list ({len(table_list)} tables provided)...")
        logger.info(f"--- STARTING HEADLESS MIGRATION (List: {table_list}, DB: {config.DB_DATABASE}) ---")
        tables = get_lib_tables(from_list=table_list)
    else:
        print(f"Using regex pattern: '{pattern}'...")
        logger.info(f"--- STARTING HEADLESS MIGRATION (Pattern: {pattern}, DB: {config.DB_DATABASE}) ---")
        tables = get_lib_tables(pattern=pattern)
        
    print(f"-> Found {len(tables)} matching tables in source database.")
    
    print(f"\n[Starting Migration]")
    run_migration(tables, current_state, suffix)
    
    print(f"\n=============================================")
    print(f"      HEADLESS MIGRATION JOB COMPLETED       ")
    print(f"=============================================")

def migration_menu(suffix):
    while True:
        print(f"\n=============================================")
        print(f"    MIGRATION OPTIONS")
        print(f"    Source DB: {config.DB_DATABASE}")
        if hasattr(config, 'DEST_DB_DATABASE'):
            print(f"    Dest DB:   {config.DEST_DB_DATABASE}")
        print(f"    Table Suffix: {suffix}")
        print(f"=============================================")
        print("1. Specify table name pattern (Regular Expression)")
        print("2. Specify exact table names (Comma-separated list)")
        print("3. Detect and resume paused session")
        print("4. Exit / Back to main menu")
        print("=============================================")
        
        choice = input("Select an option (1-4): ").strip()
        
        if choice == '1':
            pattern = input("Enter regular expression pattern (e.g., '^lib_.*'): ").strip()
            if not pattern:
                print("Pattern cannot be empty. Returning to menu.")
                continue
            
            # Start fresh state for new query
            state = {
                "processed_tables": [], 
                "migrated_tables": [], 
                "pattern": pattern, 
                "from_list": None, 
                "suffix": suffix,
                "db_host": config.DB_HOST,
                "db_user": config.DB_USER,
                "db_database": config.DB_DATABASE,
                "dest_db_host": getattr(config, 'DEST_DB_HOST', None),
                "dest_db_user": getattr(config, 'DEST_DB_USER', None),
                "dest_db_database": getattr(config, 'DEST_DB_DATABASE', None)
            }
            save_state(state)
            
            logger.info(f"--- STARTING NEW MIGRATION (Pattern: {pattern}, DB: {config.DB_DATABASE}) ---")
            tables = get_lib_tables(pattern=pattern)
            run_migration(tables, state, suffix)
            
        elif choice == '2':
            tables_input = input("Enter table names separated by commas: ").strip()
            if not tables_input:
                print("List cannot be empty. Returning to menu.")
                continue
            
            table_list = [t.strip() for t in tables_input.split(',')]
            
            # Start fresh state for new query
            state = {
                "processed_tables": [], 
                "migrated_tables": [], 
                "pattern": None, 
                "from_list": table_list, 
                "suffix": suffix,
                "db_host": config.DB_HOST,
                "db_user": config.DB_USER,
                "db_database": config.DB_DATABASE,
                "dest_db_host": getattr(config, 'DEST_DB_HOST', None),
                "dest_db_user": getattr(config, 'DEST_DB_USER', None),
                "dest_db_database": getattr(config, 'DEST_DB_DATABASE', None)
            }
            save_state(state)
            
            logger.info(f"--- STARTING NEW MIGRATION (List: {table_list}, DB: {config.DB_DATABASE}) ---")
            tables = get_lib_tables(from_list=table_list)
            run_migration(tables, state, suffix)
            
        elif choice == '3':
            if not os.path.exists(state_file):
                print("No paused session found. Start a new migration.")
                continue
                
            state = load_state()
            if not state.get("processed_tables") and not state.get("migrated_tables"):
                print("Paused session is empty. Start a new migration.")
                continue
                
            logger.info(f"--- RESUMING PAUSED MIGRATION (DB: {config.DB_DATABASE}) ---")
            
            # Re-fetch the tables based on the saved state parameters
            pattern = state.get("pattern")
            from_list = state.get("from_list")
            saved_suffix = state.get("suffix", suffix)
            
            if from_list:
                print(f"Resuming previous session based on table list: {from_list}...")
                tables = get_lib_tables(from_list=from_list) 
            else:
                p = pattern if pattern else "^lib_.*"
                print(f"Resuming previous session based on pattern: '{p}'...")
                tables = get_lib_tables(pattern=p) 
            
            run_migration(tables, state, saved_suffix)
            
        elif choice == '4':
            print("Returning...")
            break
        else:
            print("Invalid option. Please try again.")

def main():
    parser = argparse.ArgumentParser(description="Python Data Migration Utility")
    parser.add_argument('-c', '--config', help="Path to JSON configuration file for scheduled/headless execution")
    args = parser.parse_args()

    if args.config:
        run_headless(args.config)
        return

    while True:
        print("\n=============================================")
        print("         PYTHON DATA MIGRATION               ")
        print("=============================================")
        print("1. PPISv 2 (Suffix: _v2)")
        print("2. PPISv 3 (Suffix: _v3)")
        print("3. Custom Migration (Input custom suffix)")
        print("4. Restore database from SQL file(s)")
        print("5. Resume paused session")
        print("6. Exit")
        print("=============================================")
        
        main_choice = input("Select an option (1-6): ").strip()
        
        if main_choice == '6':
            print("Exiting...")
            sys.exit(0)
            
        if main_choice == '4':
            print("\n--- RESTORE DATABASE FROM SQL ---")
            
            sql_path = input("Enter the directory path containing the SQL files: ").strip()
            if not os.path.exists(sql_path) or not os.path.isdir(sql_path):
                print("Invalid directory path. Returning to menu.")
                continue
                
            file_pattern = input("Enter regular expression for filenames (e.g., '.*\\.sql$'): ").strip()
            if not file_pattern:
                print("Pattern cannot be empty. Returning to menu.")
                continue
                
            try:
                regex = re.compile(file_pattern)
            except re.error:
                print("Invalid regular expression. Returning to menu.")
                continue

            sql_files = [f for f in os.listdir(sql_path) if regex.match(f)]
            
            if not sql_files:
                print("No matching SQL files found in the specified directory.")
                continue
                
            print(f"\nFound {len(sql_files)} matching files.")
            
            print("\n--- Destination Server Connection ---")
            config.DEST_DB_HOST = input(f"Enter Destination MySQL Host IP [{config.DEST_DB_HOST}]: ").strip() or config.DEST_DB_HOST
            config.DEST_DB_USER = input(f"Username [{config.DEST_DB_USER}]: ").strip() or config.DEST_DB_USER
            config.DEST_DB_PASSWORD = getpass.getpass("Password: ")
            
            if not choose_destination_database():
                print("Failed to choose a destination database. Returning to menu.")
                continue
                
            if not create_destination_db():
                print("Failed to ensure destination database. Returning to menu.")
                continue
                
            successful_restores = 0
            for file in tqdm(sql_files, desc="Restoring SQL files", unit="file", leave=False):
                filepath = os.path.join(sql_path, file)
                if load_sql_file(filepath):
                    successful_restores += 1
                    
            print(f"\nRestore complete: {successful_restores}/{len(sql_files)} files restored.")
            continue
            
        if main_choice == '5':
            if not os.path.exists(state_file):
                print("No paused session found. Start a new migration.")
                continue
                
            state = load_state()
            if not state.get("processed_tables") and not state.get("migrated_tables"):
                print("Paused session is empty. Start a new migration.")
                continue
                
            db_host = state.get("db_host")
            db_user = state.get("db_user")
            db_database = state.get("db_database")
            dest_db_host = state.get("dest_db_host")
            dest_db_user = state.get("dest_db_user")
            saved_suffix = state.get("suffix")
            
            if not (db_host and db_user and db_database and saved_suffix):
                print("Incomplete state file. Cannot resume directly. Please start normally.")
                continue
                
            print(f"\n--- RESUMING PAUSED MIGRATION ---")
            print(f"Source Host: {db_host}")
            print(f"Source User: {db_user}")
            print(f"Source Database: {db_database}")
            
            if dest_db_host:
                print(f"Dest Host: {dest_db_host}")
            if dest_db_user:
                print(f"Dest User: {dest_db_user}")
            dest_db = state.get("dest_db_database")
            if dest_db:
                print(f"Dest Database: {dest_db}")
            
            config.DB_HOST = db_host
            config.DB_USER = db_user
            config.DB_DATABASE = db_database
            config.DB_PASSWORD = getpass.getpass("Source DB Password: ")
            
            if not dest_db_host:
                dest_db_host = input(f"Enter Destination MySQL Host IP [{config.DEST_DB_HOST}]: ").strip() or config.DEST_DB_HOST
            if not dest_db_user:
                dest_db_user = input(f"Dest Username [{config.DEST_DB_USER}]: ").strip() or config.DEST_DB_USER
                
            config.DEST_DB_HOST = dest_db_host
            config.DEST_DB_USER = dest_db_user
            
            config.DEST_DB_PASSWORD = getpass.getpass("Dest DB Password: ")
            
            try:
                conn = mysql.connector.connect(
                    host=config.DB_HOST,
                    user=config.DB_USER,
                    password=config.DB_PASSWORD,
                    database=config.DB_DATABASE,
                    connect_timeout=10
                )
                if conn.is_connected():
                    conn.close()
                    logger.info(f"--- RESUMING PAUSED MIGRATION (DB: {config.DB_DATABASE}) ---")
                    
                    dest_db = state.get("dest_db_database")
                    if dest_db:
                        config.DEST_DB_DATABASE = dest_db
                        
                    pattern = state.get("pattern")
                    from_list = state.get("from_list")
                    
                    if from_list:
                        print(f"Resuming previous session based on table list: {from_list}...")
                        tables = get_lib_tables(from_list=from_list) 
                    else:
                        p = pattern if pattern else "^lib_.*"
                        print(f"Resuming previous session based on pattern: '{p}'...")
                        tables = get_lib_tables(pattern=p) 
                    
                    run_migration(tables, state, saved_suffix)
                else:
                    print("Connection failed.")
            except Error as e:
                print(f"Authentication or connection failed: {e}")
            
            continue
            
        if main_choice not in ['1', '2', '3']:
            print("Invalid choice.")
            continue
            
        if main_choice == '1':
            suffix = '_v2'
        elif main_choice == '2':
            suffix = '_v3'
        else:
            suffix = input("Enter custom suffix (e.g., '_v4'): ").strip()
        
        # Determine Server list
        if main_choice == '1':
            servers = {
                "1": ("10.255.9.100", "PPIS v2 Production"),
                "2": ("10.255.9.104", "PPIS v2 CMS SWDI Production"),
                "3": ("10.255.9.105", "PPIS v2 Staging"),
                "4": ("127.0.0.1", "Localhost")
            }
        elif main_choice == '2':
            servers = {
                "1": ("10.10.10.96", "PPIS v3 Staging"),
                "2": ("10.255.9.111", "PPIS v3 Slave"),
                "3": ("127.0.0.1", "Localhost")
            }
        else:
            servers = {
                "1": ("127.0.0.1", "Localhost")
            }
            
        print(f"\n--- Select Server for PPIS{suffix.replace('_','')} ---")
        for key, (ip, name) in servers.items():
            print(f"{key}. {name} ({ip})")
        print("0. Enter Custom Server IP")
        
        srv_choice = input("\nSelect server: ").strip()
        
        if srv_choice == '0':
            config.DB_HOST = input("Enter MySQL Host IP: ").strip()
        elif srv_choice in servers:
            config.DB_HOST = servers[srv_choice][0]
        else:
            print("Invalid choice.")
            continue
            
        print("\n--- Source Server Authentication ---")
        config.DB_USER = input(f"Username [{config.DB_USER}]: ").strip() or config.DB_USER
        config.DB_PASSWORD = getpass.getpass("Password: ")
        
        print("\n--- Destination Server Connection ---")
        config.DEST_DB_HOST = input(f"Enter Destination MySQL Host IP [{config.DEST_DB_HOST}]: ").strip() or config.DEST_DB_HOST
        config.DEST_DB_USER = input(f"Username [{config.DEST_DB_USER}]: ").strip() or config.DEST_DB_USER
        config.DEST_DB_PASSWORD = getpass.getpass("Password: ")

        if choose_database():
            if choose_destination_database():
                migration_menu(suffix)

if __name__ == "__main__":
    main()