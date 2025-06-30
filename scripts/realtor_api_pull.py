import http.client
import json
import pandas as pd
import os

# Configuration
API_HOST = "realtor-search.p.rapidapi.com"
API_KEY = "3e88392562mshd787e5bf9ea622fp18c61ajsn2db6f6a44faf"
OUTPUT_FILE = r"C:\Users\peppe\OneDrive\Desktop\Charlie\Data_Projects\nyc-apartment-search\data\apartment_listings.csv"

# Query parameters (can make dynamic later)
API_ENDPOINT = (
    "/properties/search-rent"
    "?location=postal_code%3A11201"
    "&resultsPerPage=200"
    "&sortBy=best_match"
    "&expandSearchArea=5"
    "&prices=4000%2C%208000"
    "&bedrooms=2"
    "&bathrooms=1"
    "&homeSize=900"
    "&features=washer_dryer"
)

def fetch_apartment_data():
    conn = http.client.HTTPSConnection(API_HOST)
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST,
    }

    try:
        conn.request("GET", API_ENDPOINT, headers=headers)
        res = conn.getresponse()
        raw_data = res.read()
        json_data = json.loads(raw_data)

        return json_data.get("data", {}).get("results", [])
    except Exception as e:
        print(f"Error during API call: {e}")
        return []
    finally:
        conn.close()

def process_and_save(data, output_file):
    if not data:
        print("No data received.")
        return

    df = pd.json_normalize(data)
    print(f"Pulled {len(df)} apartment listings.")

    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

def main():
    print("Starting Realtor.com API pull...")
    data = fetch_apartment_data()
    process_and_save(data, OUTPUT_FILE)
    print("Done.")

if __name__ == "__main__":
    main()
