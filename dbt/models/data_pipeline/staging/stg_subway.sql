select
    {{ dbt_utils.generate_surrogate_key(['station_name', 'station_latitude', 'station_longitude']) }} AS SUBWAY_ID,
    subway_locations.station_name as STATION_NAME,
    subway_locations.station_latitude as STATION_LATITUDE,
    subway_locations.station_longitude as STATION_LONGITUDE
from
    {{source('subway_data','subway_locations')}} 
