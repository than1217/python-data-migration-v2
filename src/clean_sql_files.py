import os
import re
from tqdm import tqdm

def clean_sql_line(line):
    # Process column declarations
    if line.lstrip().startswith('`'):
        col_pattern = re.compile(
            r'^(\s*`[^`]+`\s+)([a-z]+(?:\([^)]+\))?(?:\s+(?:unsigned|zerofill))?)(.*)',
            flags=re.IGNORECASE
        )
        match = col_pattern.match(line)
        if match:
            col_prefix = match.group(1)
            datatype = match.group(2)
            rest_of_line = match.group(3)
            
            # Remove any existing CHARACTER SET or COLLATE declarations
            rest_of_line = re.sub(r'\s*CHARACTER SET\s+\w+', '', rest_of_line, flags=re.IGNORECASE)
            rest_of_line = re.sub(r'\s*COLLATE\s+\w+', '', rest_of_line, flags=re.IGNORECASE)
            
            datatype_lower = datatype.lower()
            string_types = ['varchar', 'char', 'enum', 'set', 'text', 'longtext', 'mediumtext', 'tinytext']
            
            # Check if datatype starts with any string type 
            is_string = any(datatype_lower.startswith(t) for t in string_types)
            
            if is_string:
                return f"{col_prefix}{datatype} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci{rest_of_line}\n"
            else:
                return f"{col_prefix}{datatype}{rest_of_line}\n"
                
    # Process table declarations
    elif line.lstrip().startswith(')') and 'ENGINE=' in line.upper():
        table_opt_match = re.search(r'^(\s*\)\s*)(.*);(.*)$', line.rstrip('\n'))
        if table_opt_match:
            prefix = table_opt_match.group(1)
            options_str = table_opt_match.group(2)
            suffix = table_opt_match.group(3) # trailing stuff like comments if any
            
            # Remove all ENGINE, CHARSET, COLLATE, ROW_FORMAT
            options_str = re.sub(r'\bENGINE\s*=\s*\w+', '', options_str, flags=re.IGNORECASE)
            options_str = re.sub(r'\b(?:DEFAULT\s+)?CHARSET\s*=\s*\w+', '', options_str, flags=re.IGNORECASE)
            options_str = re.sub(r'\bCOLLATE\s*=\s*\w+', '', options_str, flags=re.IGNORECASE)
            options_str = re.sub(r'\bROW_FORMAT\s*=\s*\w+', '', options_str, flags=re.IGNORECASE)
            
            # Remove extra spaces
            options_str = re.sub(r'\s+', ' ', options_str).strip()
            
            new_options = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC"
            if options_str:
                new_options = f"{options_str} {new_options}"
                
            return f"{prefix}{new_options};{suffix}\n"

    return line

def process_file(filepath):
    tmp_filepath = filepath + '.tmp'
    try:
        with open(filepath, 'r', encoding='utf-8') as f_in, open(tmp_filepath, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                # Fast path skip for INSERT INTO since they are large and don't contain DDL
                if not line.startswith('INSERT INTO'):
                    line = clean_sql_line(line)
                f_out.write(line)
        
        # Replace original file with cleaned file
        os.replace(tmp_filepath, filepath)
        return True
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        if os.path.exists(tmp_filepath):
            os.remove(tmp_filepath)
        return False

def main():
    # Base directory is one level up from 'src'
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(base_dir, 'output', 'processed')
    
    if not os.path.exists(processed_dir):
        print(f"Processed directory not found: {processed_dir}")
        return

    # Find all .sql files in output/processed recursively
    sql_files = []
    for root, dirs, files in os.walk(processed_dir):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))

    if not sql_files:
        print(f"No SQL files found in {processed_dir}")
        return

    print(f"Found {len(sql_files)} SQL files to clean.")
    
    success_count = 0
    for filepath in tqdm(sql_files, desc="Cleaning SQL Files", unit="file"):
        if process_file(filepath):
            success_count += 1
            
    print(f"\nSuccessfully cleaned {success_count} / {len(sql_files)} files.")

if __name__ == '__main__':
    main()
