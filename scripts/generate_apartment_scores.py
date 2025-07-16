import numpy as np
import pandas as pd
import re
from sklearn.neighbors import BallTree
from sklearn.preprocessing import MinMaxScaler
from snowflake.snowpark import Session
import ast
from dotenv import load_dotenv
import os


# Load environment variables
load_dotenv()

connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

def extract_bedrooms(details):
    try:
        details = ast.literal_eval(details) if isinstance(details, str) else details
        for item in details:
            if item.get('category') == 'Bedrooms':
                for text in item.get('text', []):
                    match = re.search(r'Bedrooms:\s*(\d+)', text)
                    if match:
                        return int(match.group(1))
    except Exception:
        pass
    return None

def extract_full_bathrooms(details):
    try:
        details = ast.literal_eval(details) if isinstance(details, str) else details
        for item in details:
            if item.get('category') == 'Bathrooms':
                for text in item.get('text', []):
                    match = re.search(r'Full Bathrooms:\s*(\d+)', text)
                    if match:
                        return int(match.group(1))
    except Exception:
        pass
    return None

def extract_total_bathrooms(details):
    try:
        details = ast.literal_eval(details) if isinstance(details, str) else details
        for item in details:
            if item.get('category') == 'Bathrooms':
                for text in item.get('text', []):
                    match = re.search(r'Total Bathrooms:\s*(\d+)', text)
                    if match:
                        return int(match.group(1))
    except Exception:
        pass
    return None

def main():
    session = Session.builder.configs(connection_parameters).create()

    # Read dbt materialized tables from Snowflake
    subway_df = session.table("stg_subway").to_pandas()
    apartment_df = session.table("stg_apartment").to_pandas()
    neighborhood_df = session.table("stg_neighborhood").to_pandas()
    apartment_images_df = session.table("stg_apartment_images").to_pandas()

    # Extract bedroom/bathroom data
    apartment_df["BEDROOMS"] = apartment_df["DETAILS"].apply(extract_bedrooms)
    apartment_df["FULL_BATHROOMS"] = apartment_df["DETAILS"].apply(extract_full_bathrooms)
    apartment_df["TOTAL_BATHROOMS"] = apartment_df["DETAILS"].apply(extract_total_bathrooms)

    apartment_df["BEDROOMS"] = pd.to_numeric(apartment_df['BEDROOMS'], errors='coerce')
    apartment_df["FULL_BATHROOMS"] = pd.to_numeric(apartment_df['FULL_BATHROOMS'], errors='coerce')
    apartment_df["TOTAL_BATHROOMS"] = pd.to_numeric(apartment_df['TOTAL_BATHROOMS'], errors='coerce')

    # Prepare BallTree and calculate distances
    subway_coords = np.deg2rad(subway_df[["STATION_LATITUDE", "STATION_LONGITUDE"]].values)
    bt = BallTree(subway_coords, metric="haversine")

    query_coords = np.deg2rad(apartment_df[["LATITUDE", "LONGITUDE"]].values)
    distances, indices = bt.query(query_coords)

    specific_location = (40.7128, -74.0060)
    specific_coords = np.deg2rad(np.array(specific_location).reshape(1, -1))
    bt_location = BallTree(specific_coords, metric="haversine")
    distances_specific, _ = bt_location.query(query_coords)

    distances_miles = distances.flatten() * 3963.13
    distances_specific_miles = distances_specific.flatten() * 3963.13

    # Add results to apartment dataframe
    apartment_df["SUBWAY_ID"] = subway_df.iloc[indices.flatten()]["SUBWAY_ID"].values
    apartment_df["CLOSEST_SUBWAY_DIST"] = distances_miles
    apartment_df["DOWNTOWN_DIST"] = distances_specific_miles

    apartment_df["POSTAL_CODE"] = apartment_df["POSTAL_CODE"].apply(
        lambda x: str(int(x)).zfill(5) if pd.notnull(x) and str(x).isdigit() else None
    )

    neighborhood_df["POSTAL_CODE"] = neighborhood_df["POSTAL_CODE"].apply(
        lambda x: str(int(x)).zfill(5) if pd.notnull(x) and str(x).isdigit() else None
    )

    # Merge neighborhood and images info
    merged_df = apartment_df.merge(neighborhood_df, on="POSTAL_CODE", how="left")
    merged_df = merged_df.merge(apartment_images_df, on="LISTING_ID", how="left")

    # Normalize relevant columns
    scaler = MinMaxScaler()
    norm_cols = ["DOWNTOWN_DIST", "CLOSEST_SUBWAY_DIST", "LIST_PRICE", "SQFT", "SAFETY_PCT"]
    normalized_values = scaler.fit_transform(merged_df[norm_cols])
    normalized_df = pd.DataFrame(normalized_values, columns=[f"NORM_{col}" for col in norm_cols])

    merged_df = pd.concat([merged_df.reset_index(drop=True), normalized_df], axis=1)

    # Calculate apartment score
    merged_df["APARTMENT_SCORE"] = (
        -0.20 * merged_df["NORM_DOWNTOWN_DIST"] +
        -0.20 * merged_df["NORM_CLOSEST_SUBWAY_DIST"] +
        0.20 * merged_df["NORM_SQFT"] +
        -0.20 * merged_df["NORM_LIST_PRICE"] +
        0.20 * merged_df["NORM_SAFETY_PCT"]
    )

    # Scale APARTMENT_SCORE between 0 and 1
    min_score = merged_df["APARTMENT_SCORE"].min()
    max_score = merged_df["APARTMENT_SCORE"].max()
    merged_df["APARTMENT_SCORE"] = (merged_df["APARTMENT_SCORE"] - min_score) / (max_score - min_score)

    fct_scores_df = merged_df[[
        "LISTING_ID", "SUBWAY_ID", "POSTAL_CODE",
        "DOWNTOWN_DIST", "CLOSEST_SUBWAY_DIST", "APARTMENT_SCORE",
        "NORM_LIST_PRICE", "NORM_SQFT", "NORM_DOWNTOWN_DIST",
        "NORM_SAFETY_PCT", "NORM_CLOSEST_SUBWAY_DIST"
    ]]

    # Save to CSV for dbt seed
    session.write_pandas(fct_scores_df, "FCT_APARTMENT_SCORES", auto_create_table=True, overwrite=True)
    print("Generated FCT_APARTMENT_SCORES table in dbt_schema")
    #fct_scores_df.to_csv("seeds/apartment_scores.csv", index=False)
    #print("Generated apartment_scores.csv in dbt/seeds folder")

    dim_apartment_df = apartment_df.copy()

    dim_apartment_df['BEDROOMS'] = apartment_df['BEDROOMS']
    dim_apartment_df['FULL_BATHROOMS'] = apartment_df['FULL_BATHROOMS']
    dim_apartment_df['TOTAL_BATHROOMS'] = apartment_df['TOTAL_BATHROOMS']
  
    # Select only descriptive columns for the dim (adjust columns as needed)
    dim_apartment_df = dim_apartment_df[[
        "LISTING_ID",
        "POSTAL_CODE",
        "LATITUDE",
        "LONGITUDE",
        "ADDRESS",
        "CITY",
        "STATE",
        "LIST_PRICE",
        "SQFT",
        "HREF",
        "PHOTOS",
        "DETAILS",
        "BEDROOMS",
        "FULL_BATHROOMS",
        "TOTAL_BATHROOMS"
    ]]

    # Write dim_apartment table to Snowflake (overwrite if exists)
    session.write_pandas(dim_apartment_df, "DIM_APARTMENT", auto_create_table=True, overwrite=True)
    print("Generated DIM_APARTMENT table in dbt_schema")


if __name__ == "__main__":
    main()