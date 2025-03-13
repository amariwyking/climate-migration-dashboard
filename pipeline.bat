@echo off
python scripts\download_raw_data.py && (
  python scripts\clean_housing_data.py && (
    python scripts\population_forecasting.py && (
      python scripts\update_database.py
    )
  )
)
echo PostgreSQL updated successfully!
pause