import http.client
import json
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
from snowflake.snowpark import Session

# create connection
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

API_HOST = "realtor-search.p.rapidapi.com"
API_KEY = os.getenv("RAPIDAPI_KEY")

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

def clean_apartment_data(df):
    COLUMNS_TO_KEEP = [
        "property_id",
        "listing_id",
        "photo_count",
        "photos",
        "list_price",
        "list_date",
        "list_price_min",
        "list_price_max",
        "href",
        "details",
        "primary_photo_href",
        "location_county_name",
        "location_neighborhoods",
        "location_address_line",
        "location_address_unit",
        "location_address_street_number",
        "location_address_street_name",
        "location_address_street_suffix",
        "location_address_city",
        "location_address_postal_code",
        "location_address_state_code",
        "location_address_state",
        "location_address_country",
        "location_address_coordinate_lat",
        "location_address_coordinate_lon",
        "description_baths",
        "description_baths_min",
        "description_baths_max",
        "description_baths_full_calc",
        "description_baths_partial_calc",
        "description_beds_min",
        "description_beds_max",
        "description_sqft",
        "description_type",
        "pet_policy_cats",
        "pet_policy_dogs",
        "pet_policy_dogs_small",
        "pet_policy_dogs_large"
    ]

    # Clean column names
    df.columns = [col.replace(".", "_").replace("-", "_").replace(" ", "_") for col in df.columns]

    # Keep only needed columns that exist in df
    cols_to_use = [col for col in COLUMNS_TO_KEEP if col in df.columns]
    df = df[cols_to_use]

    # Filter rows where description_sqft is not null or empty
    df = df[df["description_sqft"].notnull() & (df["description_sqft"].astype(str).str.strip() != "")]

    return df

def main():
    session = Session.builder.configs(connection_parameters).create()

    all_results = []
    for page in range(1, MAX_PAGES + 1):
        print(f"Fetching page {page}...")
        results = fetch_apartment_data(page)
        if not results:
            print("No more results returned, ending pagination.")
            break
        all_results.extend(results)
        print(f"Page {page} returned {len(results)} listings.")
        if len(results) < RESULTS_PER_PAGE:
            print("Last page reached (fewer results than page size).")
            break

    if all_results:
        df = pd.json_normalize(all_results)
        df = clean_apartment_data(df)
        print(f"Writing {len(df)} cleaned listings to Snowflake table 'RAW_APARTMENT_DATA'...")
        session.write_pandas(df, "RAW_APARTMENT_DATA", auto_create_table=True, overwrite=True)
        print("Data successfully written!")
    else:
        print("No new data to append.")

if __name__ == "__main__":
    main()