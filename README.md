# Python Data Migration v2

A robust, Python-based MySQL table migration utility designed to migrate database tables across environments using `mysqldump` and `mysql` CLI clients. It seamlessly extracts data, intelligently modifies table schemas (updating suffixes, collations, and storage engines), and loads the data into a destination database.

## Features

- **Cross-Platform Support**: Automatically detects Windows or Linux/macOS environments to use the correct default MySQL executables (`mysql`/`mysqldump`).
- **Interactive CLI Menu**: Easily select target databases, source patterns, and migration strategies through an interactive console interface with real-time `tqdm` progress tracking per table.
- **Smart Schema Adjustments**:
  - Automatically appends configurable suffixes (e.g., `_v2`, `_v3`) to table names.
  - Converts table engines to `InnoDB` and enforces `ROW_FORMAT=DYNAMIC`.
  - Sets table and column character sets to `utf8mb4` and collations to `utf8mb4_0900_ai_ci` without causing redundant declarations.
- **Target Selection**: Select tables using Regular Expressions (e.g., `^lib_.*`) or comma-separated exact names. Includes intelligent skipping of obsolete tables (e.g., skipping years below 2020 or `_old` suffix tables).
- **Resumable Migrations**: Built-in state tracking (`migration_state.json`) allows you to seamlessly resume migrations if interrupted or paused.
- **SQL Restore Utility**: Allows restoring a directory full of `.sql` files to a destination database via regex matching.
- **Comprehensive Logging**: Detailed execution logs are stored in `migration.log`.
- **Standalone SQL Updaters**: Includes separate scripts (`src/update.py`, `src/clean_sql_files.py`) to bulk process, update collation/engine attributes, and remove redundant schema syntax in existing `.sql` files.

## Prerequisites

- **Python 3.7+**
- **MySQL Client Binaries**: Ensure that `mysqldump` and `mysql` executable paths are available in your system's PATH, or configure their explicit paths in the configuration file.

## Installation

1. Clone this repository.
2. Install the required Python dependencies:

```bash
pip install -r requirements.txt
```

*(Dependencies include `mysql-connector-python` and `tqdm`)*

## Configuration

The database connections and executable paths are defined in `src/config.py`. You can modify this file directly or set the corresponding environment variables:

**Source Database:**
- `DB_HOST`
- `DB_DATABASE`
- `DB_USER`
- `DB_PASSWORD`

**Destination Database:**
- `DEST_DB_HOST`
- `DEST_DB_DATABASE`
- `DEST_DB_USER`
- `DEST_DB_PASSWORD`

**Executables:**
- `MYSQLDUMP_PATH` (default: `mysqldump.exe` on Windows, `mysqldump` on Linux/macOS)
- `MYSQL_PATH` (default: `mysql.exe` on Windows, `mysql` on Linux/macOS)

## Usage

The application can be run interactively or non-interactively (headless mode) for scheduled jobs.

### Interactive Mode

Start the interactive migration script by running:

```bash
python src/table_migration.py
```

### Main Menu Options:

1. **PPISv 2 (Suffix: _v2)**: Start a migration that appends `_v2` to all migrated tables.
2. **PPISv 3 (Suffix: _v3)**: Start a migration that appends `_v3` to all migrated tables.
3. **Custom Migration (Input custom suffix)**: Start a migration and prompt for a custom suffix string (e.g., `_v4`).
4. **Restore database from SQL file(s)**: Load existing `.sql` files from a specified directory into a destination database.
5. **Resume paused session**: Read the `migration_state.json` file and seamlessly continue from where a previous run left off.
6. **Exit**: Terminate the application.

**Interactive Migration Flow (Options 1, 2 & 3):**
If you choose to start a new migration, the script will guide you through:
1. **Server Selection**: Choose from predefined server IPs or enter a custom one.
2. **Authentication**: Provide credentials for the Source and Destination servers.
3. **Database Selection**: Interactively select the source database and the destination database from a list (with the option to create a new destination database).
4. **Target Selection Menu**: Choose how to select tables:
   - **Regular Expression**: e.g., `^lib_.*`
   - **Exact Table Names**: Comma-separated list of tables
   - **Resume Paused Session**: Resume a previously interrupted session with the same parameters

### Scheduled Job (Headless Mode)

You can run the script without any user interaction by providing a JSON configuration file, which is perfect for scheduled tasks (like cron jobs or Windows Task Scheduler).

```bash
python src/table_migration.py -c connection_config.json
```

**Example `connection_config.json`:**
```json
{
    "db_host": "10.255.9.104",
    "db_user": "source_username",
    "db_password": "source_password",
    "db_database": "pppp",
    "dest_db_host": "10.10.10.133",
    "dest_db_user": "dest_username",
    "dest_db_password": "dest_password",
    "dest_db_database": "consultant_ods",
    "suffix": "_v3",
    "pattern": "^lib_.*",
    "resume": true
}
```
*Note: You can specify either `pattern` (regex string) or `table_list` (comma-separated string e.g., `"table1,table2"` or JSON array `["table1", "table2"]`). If `resume` is `true`, it will skip tables already tracked in `migration_state.json`.*

### Standalone SQL Updaters

If you only need to process existing SQL dump files to update their collation and storage engines, or clean up redundant statements, use these utilities:

```bash
python src/update.py
```
This script reads all `.sql` files in `output/processed/` and standardizes them to `InnoDB` and `utf8mb4`.

```bash
python src/clean_sql_files.py
```
This script performs advanced cleanup recursively on `.sql` files in `output/processed/`, ensuring no duplicate `CHARACTER SET` or `COLLATE` definitions exist on string columns or table definitions.

## Directory Structure

```text
python-data-migration-v2/
├── output/
│   ├── raw/                 # Intermediate storage for raw mysqldump files (e.g., raw/v2, raw/v3)
│   └── processed/           # Storage for schema-modified mysqldump files (e.g., processed/v2, processed/v3)
├── src/
│   ├── config.py            # Default configuration variables
│   ├── table_migration.py   # Primary application entrypoint
│   ├── update.py            # Standalone SQL modification utility
│   └── clean_sql_files.py   # Utility to remove redundant charsets/collations
├── migration_state.json     # Migration progress tracker (auto-generated)
├── migration.log            # Event log file (auto-generated)
├── connection_config.json   # JSON config for headless execution
├── requirements.txt         # Python package requirements
└── README.md                # This document
