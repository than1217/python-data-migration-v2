import os

# Database configuration settings
DB_HOST = os.environ.get('DB_HOST', '10.255.9.104')
DB_DATABASE = os.environ.get('DB_DATABASE', 'pppp')
DB_USER = os.environ.get('DB_USER', 'jfiguero')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'jfigueroJF9(')
# MYSQLDUMP_PATH: Full path to the mysqldump.exe executable.
# Example for MySQL Workbench: "C:\\Program Files\\MySQL\\MySQL Workbench 8.0 CE\\mysqldump.exe"
# Example for XAMPP: "C:\\xampp\\mysql\\bin\\mysqldump.exe"
# NOTE: Use double backslashes (\\) in the path.
# MYSQLDUMP_PATH = os.environ.get('MYSQLDUMP_PATH', "C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqldump.exe")
MYSQLDUMP_PATH = os.environ.get('MYSQLDUMP_PATH', "mysqldump.exe")
# MYSQL_PATH: Full path to the mysql.exe executable.
MYSQL_PATH = os.environ.get('MYSQL_PATH', "mysql.exe")

# Destination database configuration
DEST_DB_HOST = os.environ.get('DEST_DB_HOST', '10.10.10.133')
DEST_DB_DATABASE = os.environ.get('DEST_DB_DATABASE', 'consultant_ods')
# DEST_DB_USER = os.environ.get('DEST_DB_USER', 'jfiguero')
# DEST_DB_PASSWORD = os.environ.get('DEST_DB_PASSWORD', 'jfigueroJF)#')

DEST_DB_USER = os.environ.get('DEST_DB_USER', 'psalonga')
DEST_DB_PASSWORD = os.environ.get('DEST_DB_PASSWORD', 'psalongaPS($')