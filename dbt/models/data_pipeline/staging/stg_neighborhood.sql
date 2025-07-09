select
    location_address_postal_code as postal_code,
    SAFETY_SCORE_LETTER_GRADE,
    SAFETY_PCT
from
    {{source('neighborhood_data','zipcodes_safety_grades_nyc')}} 
