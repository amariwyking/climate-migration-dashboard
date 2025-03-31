@echo off
python scripts\download_counties.py && (
  python scripts\download_raw_data.py && (
    python scripts\historical_population.py && (
      python scripts\population_forecasting.py && (
        python scripts\clean_data.py && (
          python scripts\update_database.py
        )
      )
    )
  )
)
echo PostgreSQL updated successfully!
pause