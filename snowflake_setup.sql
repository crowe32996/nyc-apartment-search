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

-- Grant usage on database (assuming you already recreated your apartment hunt DB)
GRANT USAGE ON DATABASE dbt_db TO ROLE dbt_role;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA dbt_db.dbt_schema TO ROLE dbt_role;

GRANT USAGE ON SCHEMA dbt_schema TO ROLE dbt_role;
GRANT ALL PRIVILEGES ON SCHEMA dbt_schema TO ROLE dbt_role;

-- Create the stage
CREATE STAGE IF NOT EXISTS MY_CSV_STAGE;

-- Grant permissions (if using a custom role like DBT_ROLE)
GRANT READ, WRITE ON STAGE MY_CSV_STAGE TO ROLE DBT_ROLE;

-- switch to admin for Airbyte permissions
USE ROLE SYSADMIN;

-- Allow dbt_role to fully operate in the database
GRANT CREATE SCHEMA ON DATABASE DBT_DB TO ROLE dbt_role;
GRANT MODIFY ON DATABASE DBT_DB TO ROLE dbt_role;  

