WITH apartment_data AS (
    SELECT
        LISTING_ID,
        images.value:href::STRING AS image_url,
        ROW_NUMBER() OVER (PARTITION BY LISTING_ID ORDER BY images.seq) AS img_rank
    FROM {{ ref('stg_apartment') }},
    LATERAL FLATTEN(input => PHOTOS) AS images  -- <-- remove PARSE_JSON
)

SELECT 
    LISTING_ID,
    image_url AS IMAGE_URL
FROM apartment_data
WHERE img_rank = 1
