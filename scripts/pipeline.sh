#!/bin/bash

python -m preprocessing.acquisition.download_counties && \
python -m preprocessing.acquisition.download_raw_data && \
python -m preprocessing.cleaning.convert_xlsx_to_csvs && \
python -m preprocessing.analysis.historical_population && \
python -m preprocessing.analysis.population_forecasting && \
python -m preprocessing.cleaning.clean_data && \
python -m preprocessing.analysis.socio_economic_index && \
python -m preprocessing.database.update_database

echo "PostgreSQL updated successfully!"
read -p "Press Enter to continue..."