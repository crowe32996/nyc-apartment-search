SELECT
    fct.LISTING_ID,
    fct.SUBWAY_ID,
    fct.POSTAL_CODE,
    fct.DOWNTOWN_DIST,
    fct.CLOSEST_SUBWAY_DIST,

    apt.LIST_PRICE,
    apt.SQFT,
    apt.BEDROOMS,
    apt.TOTAL_BATHROOMS,
    apt.FULL_BATHROOMS,
    apt.HREF,
    apt.PHOTOS,
    apt.DETAILS,
    apt.ADDRESS,
    apt.CITY,
    apt.STATE,
    apt.LATITUDE,
    apt.LONGITUDE,

    n.SAFETY_PCT,
    n.SAFETY_SCORE_LETTER_GRADE,

    s.STATION_NAME,
    s.STATION_LATITUDE,
    s.STATION_LONGITUDE,

    img.IMAGE_URL,

    fct.NORM_LIST_PRICE,
    fct.NORM_SQFT,
    fct.NORM_DOWNTOWN_DIST,
    fct.NORM_SAFETY_PCT,
    fct.NORM_CLOSEST_SUBWAY_DIST,
    fct.APARTMENT_SCORE

FROM {{ ref('fct_apartment_scores') }} fct
LEFT JOIN {{ ref('dim_apartment') }} apt
    ON fct.LISTING_ID = apt.LISTING_ID
LEFT JOIN {{ ref('dim_neighborhood') }} n
    ON fct.POSTAL_CODE = n.POSTAL_CODE
LEFT JOIN {{ ref('dim_subway') }} s
    ON fct.SUBWAY_ID = s.SUBWAY_ID
LEFT JOIN {{ ref('dim_apartment_images') }} img
    ON fct.LISTING_ID = img.LISTING_ID

WHERE apt.LIST_PRICE IS NOT NULL AND apt.SQFT IS NOT NULL