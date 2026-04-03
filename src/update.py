import os
import re
from tqdm import tqdm

def update_collation_in_file(filepath):
    """
    Reads a standalone SQL file, scans for table and column definitions, 
    updates the character set, collation, and storage engine, and saves it back.
    This script is useful for post-processing dump files that were exported manually.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Update Table-level options (ENGINE, CHARSET, COLLATE, ROW_FORMAT)
        def update_table_options(match):
            options = match.group(1)
            # Ensure ENGINE is InnoDB (Replaces MyISAM or others)
            options = re.sub(r'ENGINE\s*=\s*\w+', 'ENGINE=InnoDB', options, flags=re.IGNORECASE)
            
            # Ensure CHARSET is utf8mb4 (Appends if missing)
            if re.search(r'(?:DEFAULT\s+)?CHARSET\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'(?:DEFAULT\s+)?CHARSET\s*=\s*\w+', 'DEFAULT CHARSET=utf8mb4', options, flags=re.IGNORECASE)
            else:
                options += ' DEFAULT CHARSET=utf8mb4'
                
            # Ensure COLLATE is utf8mb4_0900_ai_ci (Appends if missing)
            if re.search(r'COLLATE\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'COLLATE\s*=\s*\w+', 'COLLATE=utf8mb4_0900_ai_ci', options, flags=re.IGNORECASE)
            else:
                options += ' COLLATE=utf8mb4_0900_ai_ci'
                
            # Ensure ROW_FORMAT is DYNAMIC (Appends if missing)
            if re.search(r'ROW_FORMAT\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'ROW_FORMAT\s*=\s*\w+', 'ROW_FORMAT=DYNAMIC', options, flags=re.IGNORECASE)
            else:
                options += ' ROW_FORMAT=DYNAMIC'
                
            return f") {options};"

        # Regex to locate the end of a CREATE TABLE statement
        content = re.sub(r'\)\s*(ENGINE\s*=[^;]+);', update_table_options, content, flags=re.IGNORECASE)

        # Replace standalone Column-level character sets and collations
        content = re.sub(r'CHARACTER SET\s+\w+', 'CHARACTER SET utf8mb4', content, flags=re.IGNORECASE)
        content = re.sub(r'COLLATE\s+\w+', 'COLLATE utf8mb4_0900_ai_ci', content, flags=re.IGNORECASE)

        # Explicitly set CHARACTER SET and COLLATE for string types (VARCHAR, ENUM, TEXT, etc.) if missing.
        # This matches column definitions like: `column_name` varchar(255) ...
        # and injects the charset and collation immediately after the data type.
        def inject_charset_collate(match):
            col_def = match.group(0)
            if re.search(r'CHARACTER SET', col_def, flags=re.IGNORECASE):
                return col_def
            return match.group(1) + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci" + match.group(2)

        pattern = r'(`[^`]+`\s+(?:varchar\([^)]+\)|char\([^)]+\)|enum\([^)]+\)|text|longtext|mediumtext|tinytext))([^,\n]*)'
        content = re.sub(pattern, inject_charset_collate, content, flags=re.IGNORECASE)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
            
        return True

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
        return False

def main():
    """
    Scans the output/processed directory for all .sql files and sequentially 
    updates their schema definitions using update_collation_in_file.
    """
    processed_dir = os.path.join('output', 'processed')
    if not os.path.isdir(processed_dir):
        print(f"Directory not found: {processed_dir}")
        return

    sql_files = [f for f in os.listdir(processed_dir) if f.endswith('.sql')]
    if not sql_files:
        print(f"No SQL files found in {processed_dir}")
        return

    print(f"Processing {len(sql_files)} files in {processed_dir}...")
    success_count = 0
    
    for filename in tqdm(sql_files, desc="Updating SQL Collations", unit="file"):
        filepath = os.path.join(processed_dir, filename)
        if update_collation_in_file(filepath):
            success_count += 1

    print(f"\nCompleted updating {success_count}/{len(sql_files)} files.")

if __name__ == "__main__":
    main()