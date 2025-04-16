@echo off

python -m climate_migration.preprocessing.acquisition.download_counties && ^
python -m climate_migration.preprocessing.acquisition.download_raw_data && ^
python -m climate_migration.preprocessing.cleaning.convert_xlsx_to_csvs && ^
python -m climate_migration.preprocessing.analysis.historical_population && ^
python -m climate_migration.preprocessing.analysis.population_forecasting && ^
python -m climate_migration.preprocessing.cleaning.clean_data && ^
python -m climate_migration.preprocessing.analysis.socio_economic_index && ^
python -m climate_migration.preprocessing.database.update_database

echo PostgreSQL updated successfully!
pause