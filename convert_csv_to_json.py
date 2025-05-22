import pandas as pd
import json

def convert_csv_to_json(csv_file='cleaned_links.csv', json_file='cleaned_links.json'):
    try:
        df = pd.read_csv(csv_file)
        url_col = next((col for col in df.columns if 'url' in col.lower()), None)
        if not url_col:
            raise ValueError("No URL column found in CSV file")

        # Get unique non-empty URLs as a list
        urls = df[url_col].dropna().unique().tolist()

        # Save to JSON file
        with open(json_file, 'w') as f:
            json.dump(urls, f, indent=2)

        print(f"✅ Converted {len(urls)} URLs from '{csv_file}' to JSON file '{json_file}'")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    convert_csv_to_json()
