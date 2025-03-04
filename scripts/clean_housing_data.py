import pandas as pd
import re
import os

# Define the columns to keep
columns_to_keep = {
  "DP04_0001E": "Total housing units",
  "DP04_0044E": "Occupied housing units",
  "DP04_0088E": "Median house value",
  "DP04_0132E": "Median gross rent"
}

colums_to_keep_from_2015 = {
  "DP04_0001E": "Total housing units",
  "DP04_0002E": "Occupied housing units",
  "DP04_0089E": "Median house value",
  "DP04_0134E": "Median gross rent"
}

all_dfs = []

# Get all files from the directory
data_dir = "C:/Code/CapstoneProject/climate-migration-dashboard/data/raw/US_counties_housing_data"
csv_files = [f for f in os.listdir(data_dir) if f.startswith("ACSDP5Y") and f.endswith("DP04-Data.csv")]

for file in csv_files:
    # Extract year
    year_match = re.search(r'(\d{4})', file)
    if not year_match:
        continue
    year = int(year_match.group(1))
    
    # Decide which columns to use
    if year < 2015:
        use_cols = columns_to_keep
    else:
        use_cols = colums_to_keep_from_2015
    
    file_path = os.path.join(data_dir, file)
    tmp_df = pd.read_csv(file_path, skiprows=[1], dtype=str, low_memory=False)
    
    # Rename and keep relevant columns
    tmp_df = tmp_df[list(use_cols.keys()) + ["GEO_ID"]]
    tmp_df = tmp_df.rename(columns=use_cols)
    # Extract last 5 digits from GEO_ID
    tmp_df["GEO_ID"] = tmp_df["GEO_ID"].str.extract(r'(\d{5})$')
    tmp_df["Year"] = year
    all_dfs.append(tmp_df)

us_data = pd.concat(all_dfs, ignore_index=True)
# Convert possibly mixed-type columns to numeric (coerce non-numbers to NaN)
us_data[list(use_cols.values())] = us_data[list(use_cols.values())].apply(pd.to_numeric, errors='coerce')

# Rename GEO_ID column to county_FIPS
us_data = us_data.rename(columns={"GEO_ID": "county_FIPS"})

us_data = us_data.set_index('county_FIPS')
print(us_data.head())

# Fix: Provide a complete file path including filename
output_path = os.path.join(r'C:\Code\CapstoneProject\climate-migration-dashboard\data\processed\cleaned_data', 'cleaned_housing_data.csv')
us_data.to_csv(output_path)
