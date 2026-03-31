import os
import re

def update_collation_in_file(filepath):
    """Reads a SQL file, updates the collation, and saves it back."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Replace Table-level character sets and collations
        content = re.sub(r'COLLATE=\w+', 'COLLATE=utf8mb4_0900_ai_ci', content, flags=re.IGNORECASE)
        
        # Replace Column-level character sets and collations
        content = re.sub(r'COLLATE \w+', 'COLLATE utf8mb4_0900_ai_ci', content, flags=re.IGNORECASE)
        
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