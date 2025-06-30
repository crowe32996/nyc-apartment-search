select
    "location.address.postal_code" as postal_code,
    "SAFETY_SCORE_LETTER_GRADE",
    "SAFETY_PCT"
from
    {{source('neighborhood_data','neighborhood_safety')}} 
