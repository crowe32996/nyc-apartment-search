select
    *
from
    {{ref('stg_my_model')}} as dim_model
where APARTMENT_SCORE IS NOT NULL