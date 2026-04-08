import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file (if it exists)
load_dotenv()

# Default Source Database Configuration
# These are used as defaults if not explicitly set via environment variables.
# When running interactively, the CLI menu allows the user to override these.
DB_HOST = os.environ.get('DB_HOST', '10.255.9.100')
DB_DATABASE = os.environ.get('DB_DATABASE', 'pppp')
DB_USER = os.environ.get('DB_USER', 'jfigueroa')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'jfigueroJF)#')

is_windows = sys.platform.startswith('win')

# System Executable Paths
# MYSQLDUMP_PATH: Full system path to the 'mysqldump' executable.
# Example for MySQL Workbench on Windows: "C:\\Program Files\\MySQL\\MySQL Workbench 8.0 CE\\mysqldump.exe"
# Example for XAMPP on Windows: "C:\\xampp\\mysql\\bin\\mysqldump.exe"
# NOTE: If passing absolute paths on Windows, ensure you use double backslashes (\\).
MYSQLDUMP_PATH = os.environ.get('MYSQLDUMP_PATH', "mysqldump.exe" if is_windows else "mysqldump")

# MYSQL_PATH: Full system path to the 'mysql' executable used for importing.
MYSQL_PATH = os.environ.get('MYSQL_PATH', "mysql.exe" if is_windows else "mysql")

# Default Destination Database Configuration
# These are used as default inputs for the destination server during interactive and headless runs.
DEST_DB_HOST = os.environ.get('DEST_DB_HOST', '10.10.10.133')
DEST_DB_DATABASE = os.environ.get('DEST_DB_DATABASE', 'consultant_ods')
DEST_DB_USER = os.environ.get('DEST_DB_USER', 'jfiguero')
DEST_DB_PASSWORD = os.environ.get('DEST_DB_PASSWORD', 'jfigueroJF)#')

# Alternative Target DB Credentials (commented out for reference)
# DEST_DB_USER = os.environ.get('DEST_DB_USER', 'psalonga')
# DEST_DB_PASSWORD = os.environ.get('DEST_DB_PASSWORD', 'psalongaPS($')