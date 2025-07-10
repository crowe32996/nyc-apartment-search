import http.client
import json
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

API_HOST = "realtor-search.p.rapidapi.com"
API_KEY = os.getenv("RAPIDAPI_KEY")
OUTPUT_FILE = r"C:\Users\peppe\OneDrive\Desktop\Charlie\Data_Projects\nyc-apartment-search\dbt\seeds\apartment_listings.csv"

RESULTS_PER_PAGE = 200
MAX_PAGES = 100  # max safety limit, adjust as needed

def fetch_apartment_data(page):
    conn = http.client.HTTPSConnection(API_HOST)
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST,
    }

    API_ENDPOINT = (
        f"/properties/search-rent"
        f"?location=postal_code%3A11201"
        f"&resultsPerPage={RESULTS_PER_PAGE}"
        f"&sortBy=best_match"
        f"&expandSearchArea=5"
        f"&homeSize=300"
        f"&page={page}"
    )

    try:
        conn.request("GET", API_ENDPOINT, headers=headers)
        res = conn.getresponse()

        # 🔍 DEBUG lines: print status and rate limit info
        print(f"Response Status: {res.status}")
        for key, value in res.getheaders():
            if "rate" in key.lower() or key.lower() in ["retry-after"]:
                print(f"{key}: {value}")

        if res.status == 429:
            print("Hit API rate limit (HTTP 429). Consider backing off or retrying later.")

        raw_data = res.read()
        json_data = json.loads(raw_data)
        return json_data.get("data", {}).get("results", [])

    except Exception as e:
        print(f"Error during API call: {e}")
        return []
    finally:
        conn.close()

def get_last_page_downloaded(output_file):
    if not os.path.exists(output_file):
        return 0
    try:
        df = pd.read_csv(output_file)
        total_rows = len(df)
        last_page = total_rows // RESULTS_PER_PAGE
        if total_rows % RESULTS_PER_PAGE != 0:
            last_page += 1
        print(f"Found {total_rows} rows in existing CSV, last page assumed: {last_page}")
        return last_page
    except Exception as e:
        print(f"Could not read existing CSV: {e}")
        return 0

def main():
    last_page = get_last_page_downloaded(OUTPUT_FILE)
    next_page = last_page + 1
    all_new_results = []

    for page in range(next_page, next_page + MAX_PAGES):
        print(f"Fetching page {page}...")
        results = fetch_apartment_data(page)
        if not results:
            print("No more results returned, ending pagination.")
            break
        all_new_results.extend(results)
        print(f"Page {page} returned {len(results)} listings.")
        if len(results) < RESULTS_PER_PAGE:
            print("Last page reached (fewer results than page size).")
            break

    if all_new_results:
        df_new = pd.json_normalize(all_new_results)
        print(f"Appending {len(df_new)} new listings to CSV.")
        # Append without headers if file exists
        df_new.to_csv(OUTPUT_FILE, mode='a', index=False, header=not os.path.exists(OUTPUT_FILE))
        print(f"Data appended to {OUTPUT_FILE}")
    else:
        print("No new data to append.")

if __name__ == "__main__":
    main()