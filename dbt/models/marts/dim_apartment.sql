select * from {{ source('apartment_data_transformed', 'dim_apartment') }}
WHERE
    LIST_PRICE <= 20000 AND SQFT <= 6000 AND BEDROOMS <= 6 AND TOTAL_BATHROOMS <= 6
    AND SQFT IS NOT NULL AND LIST_PRICE IS NOT NULL
