-- temporary placeholder
select * from {{ source('apartment_data_transformed', 'fct_apartment_scores') }}
