import re
import censusdis.data as ced

from pathlib import Path
from censusdis.datasets import ACS5


DATA_DIR = Path("./data/processed/cleaned_data/")
DATA_DIR.mkdir(parents=True, exist_ok=True)

counties = ced.download(
    dataset=ACS5,
    vintage=2020,
    download_variables=["NAME"],
    state="*",
    county="*",
    with_geometry=True,
)

# Construct COUNTY_FIPS by combaining state and county FIPS
counties["COUNTY_FIPS"] = counties["STATE"] + counties["COUNTY"]
counties.head()

counties = counties.set_index("COUNTY_FIPS")
counties = counties.rename(
    columns={
        "geometry": "GEOMETRY",
    }
)


counties.to_csv(DATA_DIR / "county.csv")
