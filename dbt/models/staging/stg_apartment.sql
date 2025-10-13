select
    listing_id AS LISTING_ID, 
    cast(list_price as numeric) as LIST_PRICE,
    href as HREF, 
    details as DETAILS,
    cast(description_sqft as int) as SQFT,
    location_address_postal_code as POSTAL_CODE,
    location_address_coordinate_lat as LATITUDE,
    location_address_coordinate_lon as LONGITUDE,
    location_address_line as ADDRESS,
    location_address_city as CITY,
    location_address_state_code as STATE,
    photos as PHOTOS
from
    {{ref('stg_apartment_raw')}} 
WHERE
    LIST_PRICE <= 20000 AND SQFT <= 6000
