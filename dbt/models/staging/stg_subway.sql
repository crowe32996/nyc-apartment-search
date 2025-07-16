select
    {{ dbt_utils.generate_surrogate_key(['station_name', 'station_latitude', 'station_longitude']) }} AS SUBWAY_ID,
    station_name as STATION_NAME,
    station_latitude as STATION_LATITUDE,
    station_longitude as STATION_LONGITUDE
from
    {{source('subway_data','raw_subway_data')}} 