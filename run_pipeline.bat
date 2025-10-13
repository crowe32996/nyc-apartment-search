@echo off
echo Activating virtual environment...
call venv\Scripts\activate

echo Installing Python dependencies...
pip install -r requirements.txt

@REM :: Prompt user to optionally run the API ingestion. ***Taken out with Airbyte layer added
@REM set /p runAPI="Would you like to refresh raw_apartment_data in Snowflake from the API? (y/n): "
@REM if /i "%runAPI%"=="y" (
@REM     echo Running Realtor.com API ingestion script and populating Snowflake with cleaned data...
@REM     python scripts\realtor_api_pull.py
@REM ) else (
@REM     echo Skipping API ingestion and cleaning step.
@REM )

echo Changing directory to dbt...
cd dbt

echo Step 1: Seeding raw data (subway, neighborhood) to Snowflake...
dbt seed

echo Step 2: Running dbt to build staging models...
dbt run --select stg_subway stg_apartment_raw stg_apartment stg_neighborhood stg_apartment_images --exclude marts.*

echo Step 3: Generating apartment scores and creating fct_apartment_scores and dim_apartment tables...
python ..\scripts\generate_apartment_scores.py

echo Step 4: Running dbt to build final models...
dbt run --exclude stg_* 

echo All done! Apartment pipeline finished.
pause
