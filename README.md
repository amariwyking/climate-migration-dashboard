# Climate Migration Dashboard

An interactive dashboard for analyzing climate-driven migration patterns across the United States.

## Prerequisites

- Python 3.8 or higher
- Docker Desktop
- US Census API key
- PostgreSQL

## Setup Instructions

### Windows

1. Clone the repository:
   ```
   git clone [repository-url]
   cd climate-migration-dashboard
   ```

2. Set up Python environment:
   ```
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Configure Docker:
   - Install Docker Desktop
   - Start Docker containers: `docker-compose up -d`
   - Stop containers when done: `docker-compose down`

4. Configure environment variables:
   - Create `.env` file in root directory
   - Add the following variables:
     ```
     US_CENSUS_API_KEY=your_api_key_here
     DATABASE_URL=your_postgres_connection_string
     ```

5. Set up secrets:
   ```
   mkdir secrets
   echo "your_postgres_password" > secrets\postgres_password.txt
   ```

6. Prepare data:
   - Ensure `.\data\raw` directory contains required manual downloads:
     - `monthly_job_openings_xlsx_data`
     - `decennial_county_population_data_1900_1990.csv`

7. Run data pipeline:
   - Windows: `.\scripts\pipeline.bat`
   - MacOS/Linux: `./scripts/pipeline.sh`

8. Launch dashboard:
   ```
   streamlit run app/main.py
   ```

## Data Structure

### Raw Data Sources

#### Automatically Downloaded
- Counties data
- Economic indicators
- Education statistics
- Housing metrics
- Population data
- State crime statistics
- State-level data

#### Manual Downloads Required
- Monthly job openings (XLSX format)
- Historical county population data (1900-1990)

### Processed Data

#### Combined Datasets
- `cleaned_economic_data.csv`
- `cleaned_education_data.csv`
- `cleaned_housing_data.csv`
- `cleaned_crime_data.csv`
- `cleaned_job_openings_data.csv`
- `socioeconomic_indices.csv`
- `socioeconomic_indices_rankings.csv`
- `timeseries_population.csv`

#### Geographic Data
- `counties_with_geometry` (yearly data)

#### Projections
- `county_population_projections.csv`

## Troubleshooting

If you encounter issues:

- Ensure Docker Desktop is running
- Verify all environment variables are set correctly
- Check if required data files exist in `.\data\raw`
- Confirm PostgreSQL connection string is valid
