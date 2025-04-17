import datacommons as dc
import pandas as pd
from pathlib import Path

county_fips_list = [
    "01001", "01003", "01005", "01007", "01009", "01011", "01013", "01015",
    "01017", "01019", "01021", "01023", "01025", "01027", "01029", "01031",
    "01033", "01035", "01037", "01039", "01041", "01043", "01045", "01047",
    "01049", "01051", "01053", "01055", "01057", "01059", "01061", "01063",
    "01065", "01067", "01069", "01071", "01073", "01075", "01077", "01079",]

year_range = range(2010, 2024)

fema_data = {}

for county_fips in county_fips_list:
  data = dc.get_stat_series(
      f"geoId/{county_fips}",
      "FemaNaturalHazardRiskIndex_NaturalHazardImpact",
  )

  # For each year in the state's data
  for year, value in data.items():
      year_int = int(year)  # Convert year to integer for comparison

      if (
          year_range[0]
          <= year_int
          <= year_range["YEARS"][1]
      ):
        fema_data[year].append(
      {
          "COUNTY_FIPS": county_fips,
          "FEMA_NRI": value,
      })

for year, data in fema_data.items():
    fema_path = Path("C:\Code\CapstoneProject\climate-migration-dashboard\data\raw\fema_nri")
    fema_path.mkdir(parents=True, exist_ok=True)
    if data:
        df = pd.DataFrame(data)
        file_path = fema_path / f"fema_nri_data{year}.csv"
        df.to_csv(file_path, index=False)
        print(f"Saved crime data for year {year} with {len(df)} records")