select distinct
    *
from
    {{ref('stg_subway')}} as subway_locations
