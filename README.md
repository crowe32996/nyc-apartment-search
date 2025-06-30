# NYC Apartment Search Dashboard

## Overview

This project provides a dynamic apartment search dashboard for NYC and surrounding areas. The end-to-end pipeline integrates the following:

- Apartment listings via the Realtor.com API 
- Safety data by zip code (static CSV)
- Subway location data (static CSV)
- Snowflake and dbt for data warehousing and transformations
- Looker Studio dashboard for interactive exploration and weighing of apartment factors

Note that in order to manage API request limits, the search is limited to the following pre-filtering:

- 2+ bedrooms
- 1+ bathroom
- 900+ Square Feet
- Between $4000 and $8000 per month (arbitrarily chosen as wide range for most 2+ bedroom units, will adjust as I include studio and 1 bedroom apartments in the future)
- Within 5 mi radius of Downtown Brooklyn (arbitrarily chosen as somewhat central across all five boroughs and surrounding cities)
- Washer/dryer in the building

Future iterations will provide additional API request limit handling, to incorporate studio and 1 bedroom apartments, and to allow the user to  filter for factors such as washer/dryer, outdoor space, etc.
---

## Features

- Pulls latest apartment listings from Realtor.com API via RapidAPI
- User-adjustable weighting for 5 key apartment factors (price, sqft, distance to subway, distance to downtown, neighborhood safety score)
- Ranked Methodology of apartment recommendations based on weighting assigned (or the default of even 20% across all five factors)
- Modular dbt project for clean data transformations
- Snowflake data warehouse integration

---

## Getting Started

### Prerequisites

- Python 3.8+
- [Snowflake account](https://www.snowflake.com/)
- Access to Realtor.com API (via RapidAPI)
- Looker Studio (Google account)

### Setup

1. Clone the repository

    ```bash
   git clone https://github.com/yourusername/nyc-apartment-search.git
   cd nyc-apartment-search
    ```


2. Create and activate a virtual environment
    
    ```bash
    python -m venv venv

    # To activate on macOS/Linux:
    source venv/bin/activate

    # To activate on Windows CMD:
    venv\Scripts\activate.bat

    # To activate on Windows PowerShell:
    .\venv\Scripts\Activate.ps1
    ```

3. Install dependencies

    ```bash
    pip install -r requirements.txt
    ```

4. Configure Snowflake Credentials

    Set your Snowflake connection info as environment variables or in a config file.

5. Run the API ingestion script to fetch apartment listings

    ```bash
    python realtor_api_pull.py
    ```

6. Run dbt transformations

    ```bash
    dbt run
    ```

7. Open the Looker Studio dashboard and connect to your Snowflake data

## 📊 NYC Apartments Dashboard

Explore the interactive Looker Studio dashboard:

🔗 **[View the NYC Apartment Search Dashboard](https://lookerstudio.google.com/u/0/reporting/9044b3e3-d3e2-41a0-b329-0b4d23c04764)**

### Features:
- **Adjustable weights** for key apartment factors like rent, square footage, safety, distance to subway, and distance to downtown
- **Real-time filtering** by price range, neighborhood, and features
- **Color-coded heatmaps** showing how listings rank across different dimensions
- **Interactive tables** with apartment previews, links, and normalized scores


### Full Dashboard

![Full Dashboard](images/lookerstudio_nyc_apartments.png)

### Additional Screenshots:

| Apartment Ranking Table | Neighborhood Summary |
|-------------------------|----------------------|
| ![Ranked Listings](images/apartment_listings.png) | ![Neighborhood Averages](images/neighborhood_avgs.png) |

> 📌 To keep this working smoothly, periodically update the Snowflake extract or re-run the API ingestion script.


---

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Contact

Your Name - cwr321@gmail.com  
Project Link: https://github.com/crowe32996/nyc-apartment-search
