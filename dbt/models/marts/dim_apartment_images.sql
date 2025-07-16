SELECT 
    LISTING_ID,
    IMAGE_URL
FROM {{ ref('stg_apartment_images') }}