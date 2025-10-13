with raw as (
    select *
    from {{ source('airbyte', 'listings') }}
),

flattened as (
    select
        property_id,
        listing_id,
        photo_count,
        primary_photo:"href"::string as primary_photo_href,
        photos,
        list_price,
        list_date,
        href,
        details,
        location:"county":"name"::string as location_county_name,
        location:"neighborhoods"::string as location_neighborhoods,
        location:"address":"line"::string as location_address_line,
        location:"address":"unit"::string as location_address_unit,
        location:"address":"street_number"::string as location_address_street_number,
        location:"address":"street_name"::string as location_address_street_name,
        location:"address":"street_suffix"::string as location_address_street_suffix,
        location:"address":"city"::string as location_address_city,
        location:"address":"postal_code"::string as location_address_postal_code,
        location:"address":"state_code"::string as location_address_state_code,
        location:"address":"state"::string as location_address_state,
        location:"address":"country"::string as location_address_country,
        location:"address":"coordinate":"lat"::float as location_address_coordinate_lat,
        location:"address":"coordinate":"lon"::float as location_address_coordinate_lon,
        
        description:"baths"::float as description_baths,
        description:"baths_full_calc"::float as description_baths_full_calc,
        description:"baths_partial_calc"::float as description_baths_partial_calc,
        description:"beds"::float as description_beds_min,
        description:"beds"::float as description_beds_max,
        description:"sqft"::float as description_sqft,
        description:"type"::string as description_type,
        
        pet_policy:"cats"::boolean as pet_policy_cats,
        pet_policy:"dogs"::boolean as pet_policy_dogs,
        pet_policy:"dogs_small"::boolean as pet_policy_dogs_small,
        pet_policy:"dogs_large"::boolean as pet_policy_dogs_large

    from raw
)

select *
from flattened
where description_sqft is not null
