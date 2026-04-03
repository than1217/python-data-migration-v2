# Python Data Migration v2

A robust, Python-based MySQL table migration utility designed to migrate database tables across environments using `mysqldump` and `mysql` CLI clients. It seamlessly extracts data, intelligently modifies table schemas (updating suffixes, collations, and storage engines), and loads the data into a destination database.

## Features

- **Interactive CLI Menu**: Easily select target databases, source patterns, and migration strategies through an interactive console interface.
- **Smart Schema Adjustments**:
  - Automatically appends configurable suffixes (e.g., `_v2`, `_v3`) to table names.
  - Converts table engines to `InnoDB` and enforces `ROW_FORMAT=DYNAMIC`.
  - Sets table and column character sets to `utf8mb4` and collations to `utf8mb4_0900_ai_ci`.
- **Target Selection**: Select tables using Regular Expressions (e.g., `^lib_.*`) or comma-separated exact names. Includes intelligent skipping of obsolete tables (e.g., skipping years below 2020 or `_old` suffix tables).
- **Resumable Migrations**: Built-in state tracking (`migration_state.json`) allows you to seamlessly resume migrations if interrupted or paused.
- **SQL Restore Utility**: Allows restoring a directory full of `.sql` files to a destination database via regex matching.
- **Comprehensive Logging**: Detailed execution logs are stored in `migration.log`.
- **Standalone SQL Updater**: Includes a separate script (`src/update.py`) to bulk process and update collation and engine attributes of existing `.sql` files.

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
- `MYSQLDUMP_PATH` (default: `mysqldump.exe`)
- `MYSQL_PATH` (default: `mysql.exe`)

## Usage

Start the interactive migration script by running:

```bash
python src/main.py
```

### Main Menu Options:

1. **PPISv 2 (Suffix: _v2)**: Start a migration that appends `_v2` to all migrated tables.
2. **PPISv 3 (Suffix: _v3)**: Start a migration that appends `_v3` to all migrated tables.
3. **Restore database from SQL file(s)**: Load existing `.sql` files from a specified directory into a destination database.
4. **Detect and resume paused session**: Read the `migration_state.json` file and continue from where a previous run left off.
5. **Exit**: Terminate the application.

When migrating (Option 1 or 2), you'll be guided through setting source and destination database credentials interactively, selecting tables, and watching the progress via progress bars.

### Standalone SQL Updater

If you only need to process existing SQL dump files to update their collation and storage engines, run:

```bash
python src/update.py
```
This script reads all `.sql` files in `output/processed/` and standardizes them to `InnoDB` and `utf8mb4`.

## Directory Structure

```text
python-data-migration-v2/
├── output/
│   ├── raw/             # Intermediate storage for raw mysqldump files
│   └── processed/       # Storage for schema-modified mysqldump files
├── src/
│   ├── config.py        # Configuration variables
│   ├── main.py          # Primary application entrypoint
│   └── update.py        # Standalone SQL modification utility
├── migration_state.json # Migration progress tracker (auto-generated)
├── migration.log        # Event log file (auto-generated)
├── requirements.txt     # Python package requirements
└── README.md            # This document
```
