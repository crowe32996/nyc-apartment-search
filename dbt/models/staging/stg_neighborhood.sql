select
    LPAD(CAST(location_address_postal_code AS STRING), 5, '0') AS POSTAL_CODE,
    SAFETY_SCORE_LETTER_GRADE,
    SAFETY_PCT
from
    {{source('neighborhood_data','raw_neighborhood_data')}} 
