#!/bin/bash

python -m climate_migration.preprocessing.acquisition.download_counties && \
python -m climate_migration.preprocessing.acquisition.download_raw_data && \
python -m climate_migration.preprocessing.analysis.historical_population && \
python -m climate_migration.preprocessing.analysis.population_forecasting && \
python -m climate_migration.preprocessing.cleaning.clean_data && \
python -m climate_migration.preprocessing.database.update_database

echo "PostgreSQL updated successfully!"
read -p "Press Enter to continue..."