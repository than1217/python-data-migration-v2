import os
import re

def update_collation_in_file(filepath):
    """Reads a SQL file, updates the collation, and saves it back."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Replace MyISAM with InnoDB
        content = re.sub(r'ENGINE=MyISAM', 'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci', content, flags=re.IGNORECASE)
        
        # Explicitly set the Engine, Default Charset, and Collation for the whole table
        # Matches the end of the CREATE TABLE statement: `) ENGINE=... DEFAULT CHARSET=...;`
        # and standardizes it to `) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`
        table_end_pattern = r'\)\s*ENGINE\s*=\s*\w+(?:\s+DEFAULT\s+CHARSET\s*=\s*\w+)?(?:\s+COLLATE\s*=\s*\w+)?(?:\s+ROW_FORMAT\s*=\s*\w+)?\s*;'
        
        # A simpler fallback for replacing just the ENGINE part if the full match isn't found
        content = re.sub(table_end_pattern, r') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;', content, flags=re.IGNORECASE)

        # Catch cases where no CHARSET was specified at all, just `) ENGINE=InnoDB;`
        content = re.sub(r'\)\s*ENGINE\s*=\s*InnoDB\s*;\s*', r') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n', content, flags=re.IGNORECASE)
        
        # Replace Table-level character sets and collations if they are defined differently
        content = re.sub(r'CHARSET=\w+', 'CHARSET=utf8mb4', content, flags=re.IGNORECASE)
        content = re.sub(r'COLLATE=\w+', 'COLLATE=utf8mb4_0900_ai_ci', content, flags=re.IGNORECASE)
        
        # Replace Column-level character sets and collations
        content = re.sub(r'COLLATE \w+', 'COLLATE utf8mb4_0900_ai_ci', content, flags=re.IGNORECASE)

        # Explicitly set CHARACTER SET and COLLATE for VARCHAR, ENUM, TEXT, LONGTEXT, etc. if missing
        # This matches column definitions like: `column_name` varchar(255) ...
        # and appends CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci before constraints like NOT NULL or DEFAULT.
        def inject_charset_collate(match):
            col_def = match.group(0)
            # If the column already explicitly defines a CHARACTER SET, don't double inject
            if re.search(r'CHARACTER SET', col_def, flags=re.IGNORECASE):
                return col_def
            
            # The pattern is: (type definition) (the rest of the column definition)
            # e.g. (`col` varchar(255)) ( NOT NULL)
            # We insert the charset and collate in between.
            return match.group(1) + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci" + match.group(2)

        # Match columns that are string types: varchar, char, enum, text, longtext, mediumtext, tinytext
        # Group 1: The column name and data type part (e.g., `col` varchar(255) or `col` enum('A','B'))
        # Group 2: The rest of the line until the comma or end of definition
        pattern = r'(`[^`]+`\s+(?:varchar\([^)]+\)|char\([^)]+\)|enum\([^)]+\)|text|longtext|mediumtext|tinytext))([^,\n]*)'
        
        content = re.sub(pattern, inject_charset_collate, content, flags=re.IGNORECASE)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
            
        print(f"Updated collation in {filepath}")

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

def main():
    processed_dir = os.path.join('output', 'processed')
    if not os.path.isdir(processed_dir):
        print(f"Directory not found: {processed_dir}")
        return

    for filename in os.listdir(processed_dir):
        if filename.endswith('.sql'):
            filepath = os.path.join(processed_dir, filename)
            update_collation_in_file(filepath)

if __name__ == "__main__":
    main()