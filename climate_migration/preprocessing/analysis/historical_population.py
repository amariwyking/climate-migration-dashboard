import pandas as pd
import datacommons_pandas as dcpd
import censusdis.data as ced

from censusdis.datasets import ACS5
from pathlib import Path

DATA_DIR = Path("./data/")

# Get counties and their names
counties = ced.download(
    dataset=ACS5,
    vintage=2020,
    download_variables=["NAME"],
    state="*",
    county="*",
    with_geometry=True,
)

# Format the county FIPS and Data Commons DCID
counties["COUNTY_FIPS"] = counties["STATE"] + counties["COUNTY"]
counties["COUNTY_DCID"] = "geoId/" + counties["COUNTY_FIPS"]

# Set index to the county FIPS
counties = counties.set_index("COUNTY_FIPS")

# Read historical population from locally stored Census data file (seemingly unavailable via API call)
population_1900s = pd.read_csv(DATA_DIR / "raw/decennial_county_population_data_1900_1990.csv", dtype=str)

population_1900s = population_1900s[population_1900s["fips"].str[-3:] != "000"]
population_1900s = population_1900s.set_index("fips").drop(columns=["name"])
population_1900s = population_1900s.replace(".", None)
population_1900s = population_1900s.apply(pd.to_numeric, errors="coerce")

# Merge the 20th century data
counties = counties.merge(population_1900s, how="inner", left_index=True, right_index=True)

# Query Data Commons for population data
population_2000s = dcpd.build_time_series_dataframe(counties.COUNTY_DCID, "Count_Person")
population_2000s = population_2000s[["2000", "2010", "2020"]]

# Merge the 20th century data
counties = counties.merge(population_2000s[["2000", "2010", "2020"]], how="inner", left_on="COUNTY_DCID", right_index=True)
counties = counties.rename(columns=
    {
        "2000": "pop2000",
        "2010": "pop2010",
        "2020": "pop2020",
    }
)

# Name the index
counties = counties.set_index(counties.index.set_names("COUNTY_FIPS"))

# Identify the population columns
pop_columns = counties.columns[counties.columns.str.contains("pop")]

counties = counties[pop_columns]

# Create rename dictionary and apply it in one step
counties = counties.rename(columns={
    col: col[3:] for col in counties.columns 
    if col.startswith("pop") and col[3:].isdigit()
})

# Export the population columns indexed by COUNTY_FIPS
counties.to_csv(DATA_DIR / "preprocessed/cleaned_data/timeseries_population.csv")