select
    listing_id AS LISTING_ID, 
    list_price as LIST_PRICE,
    href as HREF, 
    details as DETAILS,
    description_sqft as SQFT,
    location_address_postal_code as POSTAL_CODE,
    location_address_coordinate_lat as LATITUDE,
    location_address_coordinate_lon as LONGITUDE,
    location_address_line as ADDRESS,
    location_address_city as CITY,
    location_address_state_code as STATE,
    photos as PHOTOS
from
    {{source('apartment_data','apartment_listings2')}} 
