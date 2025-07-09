import pandas as pd
import os

SEEDS_FOLDER = "C:/Users/peppe/OneDrive/Desktop/Charlie/Data_Projects/nyc-apartment-search/dbt/seeds"

# EXACT columns to keep for apartment_listings2.csv
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

for filename in os.listdir(SEEDS_FOLDER):
    if filename.endswith(".csv"):
        filepath = os.path.join(SEEDS_FOLDER, filename)
        df = pd.read_csv(filepath)

        # Clean column names: replace dots, dashes, spaces with underscores
        df.columns = [col.replace(".", "_").replace("-", "_").replace(" ", "_") for col in df.columns]

        if filename == "apartment_listings2.csv":
            # Filter columns to only those in COLUMNS_TO_KEEP and present in df
            cols_to_use = [col for col in COLUMNS_TO_KEEP if col in df.columns]
            df = df[cols_to_use]

            # Filter rows where description_sqft is populated (not null/empty)
            df = df[df["description_sqft"].notnull() & (df["description_sqft"].astype(str).str.strip() != "")]

        # Save back cleaned (and filtered if apartment_listings2.csv)
        df.to_csv(filepath, index=False)
        print(f"Processed {filename}")