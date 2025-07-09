-- Create role if needed
CREATE ROLE IF NOT EXISTS dbt_role;

-- Grant role to your user
GRANT ROLE dbt_role TO USER crowe32996;

-- Optional: Create warehouse if needed
CREATE WAREHOUSE IF NOT EXISTS dbt_wh 
  WITH WAREHOUSE_SIZE = XSMALL 
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE;

-- Grant usage to role
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_role;

CREATE DATABASE IF NOT EXISTS DBT_DB;
CREATE SCHEMA IF NOT EXISTS dbt_schema;
SELECT * FROM MY_MODEL

-- Grant usage on database (assuming you already recreated your apartment hunt DB)
GRANT USAGE ON DATABASE dbt_db TO ROLE dbt_role;
GRANT USAGE ON SCHEMA dbt_schema TO ROLE dbt_role;
GRANT ALL PRIVILEGES ON SCHEMA dbt_schema TO ROLE dbt_role;
