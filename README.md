# climate-migration-dashboard

## Windows Setup

1. Clone the repo.
2. Create a virtual environment: `python -m venv .venv`.
3. Activate it: `.venv\Scripts\activate`.
4. Install dependencies: `pip install -r requirements.txt`.
5. Install Docker desktop.
6. Run `docker-compose up -d` to run container and `docker-compose down` to stop container when done with project.
7. Create an .env and file and add your `US_CENSUS_API_KEY` and postgresSQL `DATABASE_URL` to it.
8. Create directory and txt file to save postgress password `secrets\postgres_passwod.txt`.
9. Make sure `.\data\raw` directory have manually downlaoded data in it which was not available through scripts.
10. Run `.\scripts\pipeline.bat` for Windows or `./scripts/pipeline.sh` for MacOS & Linux.
11. To start the dashboard, run `streamlit run app/main.py` from root directory.


## Data Structure

### Raw Data

- **Downloaded using scripts**:
  - `counties_data`
  - `economic_data`
  - `education_data`
  - `housing_data`
  - `population_data`
  - `state_crime_data`
  - `state_data`

- **Downloaded manually**:
  - `monthly_job_openings_xlsx_data`
  - `decennial_county_population_data_1900_1990.csv`

### Cleaned Data

- **Processed**:
  - **Combined and saved**:
    - `cleaned_economic_data.csv`
    - `cleaned_education_data.csv`
    - `cleaned_housing_data.csv`
    - `cleaned_crime_data.csv`
    - `cleaned_job_openings_data.csv`
    - `socioeconomic_indices.csv`
    - `socioeconomic_indices_rankings.csv`
    - `timeseries_population.csv`
  - **Separate yearly data saved**:
    - `counties_with_geometry`

- **Projected Data**:
  - `county_population_projections.csv`
  