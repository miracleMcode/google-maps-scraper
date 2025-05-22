import pandas as pd
import json
import re

def convert_csv_to_json(csv_file='cleaned_links.csv', json_file='cleaned_links.json'):
    try:
        df = pd.read_csv(csv_file)
        url_col = next((col for col in df.columns if 'url' in col.lower()), None)
        
        if not url_col:
            raise ValueError("No column with 'url' found in CSV.")

        # Clean and sanitize URLs
        links = (
            df[url_col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.replace(r'\s+', '', regex=True)  # Remove whitespace/newlines
            .tolist()
        )

        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(links, f, indent=2)

        print(f"✅ Converted {len(links)} URLs from '{csv_file}' to JSON file '{json_file}'")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    convert_csv_to_json()
