select
    listing_id AS LISTING_ID, 
    list_price as LIST_PRICE,
    href as HREF, 
    details as DETAILS,
    "description.sqft" as SQFT,
    "location.address.postal_code" as POSTAL_CODE,
    "location.address.coordinate.lat" as LATITUDE,
    "location.address.coordinate.lon" as LONGITUDE,
    "location.address.line" as ADDRESS,
    "location.address.city" as CITY,
    "location.address.state_code" as STATE,
    photos as PHOTOS
from
    {{source('apartment_data','apartment_listings')}} 
