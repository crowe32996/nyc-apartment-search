select
    *
from
    {{ref('stg_subway')}} as subway_locations
order by subway_id