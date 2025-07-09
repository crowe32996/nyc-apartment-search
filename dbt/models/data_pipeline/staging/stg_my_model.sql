select
    *
from
    {{ ref('apartment_scores') }}
where APARTMENT_SCORE IS NOT NULL