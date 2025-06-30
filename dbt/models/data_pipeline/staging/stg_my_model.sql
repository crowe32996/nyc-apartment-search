select
    *
from
    {{ref('my_model')}}
where APARTMENT_SCORE IS NOT NULL