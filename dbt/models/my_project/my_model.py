import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree
from sklearn.preprocessing import MinMaxScaler
from snowflake.snowpark.functions import col, lit

def model(dbt, session):
    dbt.config(materialized="table")

    subway_df = dbt.ref("stg_subway")
    apartment_df = dbt.ref("stg_apartment")
    neighborhood_df = dbt.ref("stg_neighborhood")
    apartment_images_df = dbt.ref("stg_apartment_images")

    apartment_df_pandas = apartment_df.select(
        "LISTING_ID", "LATITUDE", "LONGITUDE", "POSTAL_CODE",
        "LIST_PRICE", "SQFT", "HREF", "DETAILS", "ADDRESS", "CITY", "STATE"
    ).to_pandas()
    
    query_lats = apartment_df_pandas["LATITUDE"].values
    query_lons = apartment_df_pandas["LONGITUDE"].values

    subway_df_pandas = subway_df.select(
        "SUBWAY_ID", "STATION_LATITUDE", "STATION_LONGITUDE", "STATION_NAME"
    ).to_pandas()

    neighborhood_df_pandas = neighborhood_df.select(
        "POSTAL_CODE", "SAFETY_SCORE_LETTER_GRADE", "SAFETY_PCT"
    ).to_pandas()

    subway_coords = np.deg2rad(subway_df_pandas[["STATION_LATITUDE", "STATION_LONGITUDE"]].values)
    bt = BallTree(subway_coords, metric="haversine")
    distances, indices = bt.query(np.deg2rad(np.c_[query_lats, query_lons]))

    specific_location = (40.7128, -74.0060)
    specific_coords = np.deg2rad(np.array(specific_location).reshape(1, -1))
    bt_location = BallTree(specific_coords, metric="haversine")
    distances_specific, _ = bt_location.query(np.deg2rad(np.c_[query_lats, query_lons]))

    distances_miles = distances * 3963.13
    distances_specific_miles = distances_specific.flatten() * 3963.13

    result_df = apartment_df_pandas.copy()
    result_df["SUBWAY_ID"] = subway_df_pandas.iloc[indices.flatten()]["SUBWAY_ID"].values
    result_df["CLOSEST_SUBWAY_DIST"] = distances_miles.flatten()
    result_df["DOWNTOWN_DIST"] = distances_specific_miles

    apartment_images_df_pandas = apartment_images_df.select("LISTING_ID", "IMAGE_URL").to_pandas()

    result_df = result_df.merge(neighborhood_df_pandas, on="POSTAL_CODE", how="left")
    result_df = result_df.merge(apartment_images_df_pandas, on="LISTING_ID", how="left")

    result_snowpark_df = session.create_dataframe(result_df)
    subway_snowpark_df = session.create_dataframe(subway_df_pandas)

    result_snowpark_df = result_snowpark_df.join(
        subway_snowpark_df,
        result_snowpark_df["SUBWAY_ID"] == subway_snowpark_df["SUBWAY_ID"],
        how="left"
    ).with_column(
        "CLOSEST_SUBWAY_STOP", subway_snowpark_df["STATION_NAME"]
    )

    neighborhood_snowpark_df = session.create_dataframe(neighborhood_df_pandas)
    result_snowpark_df = result_snowpark_df.join(
        neighborhood_snowpark_df,
        result_snowpark_df["POSTAL_CODE"] == neighborhood_snowpark_df["POSTAL_CODE"],
        how="left"
    )

    # Save original values
    result_df["ORIG_DOWNTOWN_DIST"] = result_df["DOWNTOWN_DIST"]
    result_df["ORIG_CLOSEST_SUBWAY_DIST"] = result_df["CLOSEST_SUBWAY_DIST"]
    result_df["ORIG_LIST_PRICE"] = result_df["LIST_PRICE"]
    result_df["ORIG_SQFT"] = result_df["SQFT"]
    result_df["ORIG_SAFETY_PCT"] = result_df["SAFETY_PCT"]

    scaler = MinMaxScaler()
    normalized_values = scaler.fit_transform(result_df[[
        "DOWNTOWN_DIST", "CLOSEST_SUBWAY_DIST", "LIST_PRICE", "SQFT", "SAFETY_PCT"
    ]])
    normalized_df = pd.DataFrame(normalized_values, columns=[
        "NORM_DOWNTOWN_DIST", "NORM_CLOSEST_SUBWAY_DIST", "NORM_LIST_PRICE",
        "NORM_SQFT", "NORM_SAFETY_PCT"
    ])

    result_df = pd.concat([result_df.reset_index(drop=True), normalized_df], axis=1)

    result_df["APARTMENT_SCORE"] = (
        -0.20 * result_df["NORM_DOWNTOWN_DIST"] +
        -0.20 * result_df["NORM_CLOSEST_SUBWAY_DIST"] +
        0.20 * result_df["NORM_SQFT"] +
        -0.20 * result_df["NORM_LIST_PRICE"] +
        0.20 * result_df["NORM_SAFETY_PCT"]
    )

    min_score = result_df["APARTMENT_SCORE"].min()
    max_score = result_df["APARTMENT_SCORE"].max()
    result_df["APARTMENT_SCORE"] = (result_df["APARTMENT_SCORE"] - min_score) / (max_score - min_score)

    final_snowpark_df = session.create_dataframe(result_df)
    final_snowpark_df = final_snowpark_df.filter(final_snowpark_df["APARTMENT_SCORE"].is_not_null())

    return final_snowpark_df
