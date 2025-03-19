import pandas as pd
import datacommons_pandas as dcpd

DATA_DIR = "/Users/amarigarrett/Developer/climate-migration-dashboard/data/"

# Get county names from local file
counties = pd.read_csv(DATA_DIR + "raw/county_names.csv")

# Format the county FIPS and Data Commons DCID
counties["COUNTY_FIPS"] = counties["COUNTY_FIPS"].astype(str).str.zfill(5)
counties["COUNTY_DCID"] = "geoId/" + counties["COUNTY_FIPS"]

# Set index to the county FIPS
counties = counties.set_index('COUNTY_FIPS')

# Read historical population from locally stored Census data file (not available via API call)
population_1900s = pd.read_csv(DATA_DIR + "raw/decennial_county_population_data_1900_1990.csv", dtype=str)

population_1900s = population_1900s[population_1900s["fips"].str[-3:] != "000"]
population_1900s = population_1900s.set_index("fips").drop(columns=["name"])
population_1900s = population_1900s.replace('.', None)
population_1900s = population_1900s.apply(pd.to_numeric, errors='coerce')

# Merge the 20th century data
counties = counties.merge(population_1900s, how='inner', left_index=True, right_index=True)

# Query Data Commons for population data
population_2000s = dcpd.build_time_series_dataframe(counties.COUNTY_DCID, 'Count_Person')
population_2000s = population_2000s[['2000', '2010', '2020']]

# Merge the 20th century data
counties = counties.merge(population_2000s[['2000', '2010', '2020']], how='inner', left_on='COUNTY_DCID', right_index=True)
counties = counties.rename(columns=
    {
        '2000': 'pop2000',
        '2010': 'pop2010',
        '2020': 'pop2020',
    }
)

counties.to_csv(DATA_DIR + 'processed/cleaned_data/timeseries_population.csv')