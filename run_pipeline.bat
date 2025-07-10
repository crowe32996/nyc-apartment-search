@echo off
echo Activating virtual environment...
call venv\Scripts\activate

echo Installing Python dependencies...
pip install -r requirements.txt

:: Prompt user to optionally run the API ingestion
set /p runAPI="Would you like to refresh apartment listings from the API? (y/n): "
if /i "%runAPI%"=="y" (
    echo Running Realtor.com API ingestion script...
    python scripts\realtor_api_pull.py

    echo Cleaning apartment listings data...
    python scripts\clean_data.py
) else (
    echo Skipping API ingestion and cleaning step.
)

echo Changing directory to dbt...
cd dbt

echo Step 1: Running dbt to build staging models...
dbt run --full-refresh --select data_pipeline.staging

echo Step 2: Generating apartment scores CSV...
python ..\scripts\generate_apartment_scores.py

echo Step 3: Seeding CSV to Snowflake...
dbt seed

echo Step 4: Running dbt again to build final models...
dbt run

echo All done! Apartment pipeline finished.
pause
