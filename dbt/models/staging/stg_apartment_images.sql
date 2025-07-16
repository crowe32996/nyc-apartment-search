-- models/staging/stg_apartment_images.sql
WITH apartment_data AS (
    SELECT
        LISTING_ID,
        -- Parse the PHOTOS field as a VARIANT
        images.value:href::STRING AS image_url,
        ROW_NUMBER() OVER (PARTITION BY LISTING_ID ORDER BY images.seq) AS img_rank
    FROM {{ ref('stg_apartment') }},
    LATERAL FLATTEN(input => PARSE_JSON(PHOTOS)) AS images
)

SELECT 
    LISTING_ID,
    image_url AS IMAGE_URL
FROM apartment_data
WHERE img_rank = 1