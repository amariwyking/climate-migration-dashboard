from pathlib import Path
import pandas as pd
import re
from typing import Dict, List

# Configuration constants
PATHS = {
    "processed": Path("../data/processed/cleaned_data"),
    "raw_housing": Path("../data/raw/US_counties_housing_data_new"),
    "raw_population": Path("../data/raw/population_data"),
}

COLUMN_MAPPINGS = {
    (2010, 2014): {
        "DP04_0001E": "TOTAL_HOUSING_UNITS",
        "DP04_0044E": "OCCUPIED_HOUSING_NOTESD",
        "DP04_0088E": "MEDIAN_HOUSING_VALUE",
        "DP04_0132E": "MEDIAN_GROSS_RENT",
    },
    (2015, 2023): {
        "DP04_0001E": "TOTAL_HOUSING_UNITS",
        "DP04_0002E": "OCCUPIED_HOUSING_NOTESD",
        "DP04_0089E": "MEDIAN_HOUSING_VALUE",
        "DP04_0134E": "MEDIAN_GROSS_RENT",
    },
}

COMMON_COLUMNS = ["STATE", "COUNTY"]
POPULATION_COLUMN = "B01003_001E"


def get_year_from_filename(filename: str) -> int:
    """Extract year from filename using regex pattern."""
    match = re.search(r"(\d{4})", filename)
    return int(match.group(1)) if match else None


def load_and_process_housing_data() -> pd.DataFrame:
    """Load and process housing data from raw CSV files."""
    all_dfs = []

    for file in PATHS["raw_housing"].iterdir():
        if not file.is_file() or file.suffix != ".csv":
            continue

        year = get_year_from_filename(file.name)
        if not year:
            continue

        # Select appropriate column mapping
        column_map = next(
            (
                mapping
                for (start, end), mapping in COLUMN_MAPPINGS.items()
                if start <= year <= end
            ),
            None,
        )
        if not column_map:
            continue

        # Read and process data
        df = pd.read_csv(file, skiprows=[1], dtype=str, low_memory=False)
        df = process_housing_dataframe(df, column_map, year)
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


def process_housing_dataframe(
    df: pd.DataFrame, column_map: Dict[str, str], year: int
) -> pd.DataFrame:
    """Process individual housing dataframe."""
    columns = list(column_map.keys()) + COMMON_COLUMNS
    processed_df = df[columns].rename(columns=column_map)

    processed_df["COUNTY_FIPS"] = processed_df["STATE"] + processed_df["COUNTY"]
    processed_df["Year"] = year

    numeric_cols = list(column_map.values())
    processed_df[numeric_cols] = processed_df[numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    )

    return processed_df.drop(columns=COMMON_COLUMNS)


def load_population_data() -> pd.DataFrame:
    """Load and process population data from raw CSV files."""
    pop_frames = []

    for file in PATHS["raw_population"].iterdir():
        if not file.is_file() or "county" not in file.name.lower():
            continue

        year = get_year_from_filename(file.name)
        if not year:
            continue

        df = pd.read_csv(file, dtype=str, low_memory=False)
        processed_df = process_population_dataframe(df, year)
        pop_frames.append(processed_df)

    return pd.concat(pop_frames, ignore_index=True) if pop_frames else pd.DataFrame()


def process_population_dataframe(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Process individual population dataframe."""
    df["COUNTY_FIPS"] = df["STATE"] + df["COUNTY"]
    df["Year"] = year
    return df.rename(columns={POPULATION_COLUMN: "POPULATION"})[
        ["COUNTY_FIPS", "Year", "POPULATION"]
    ]


def main():
    # Create output directory
    PATHS["processed"].mkdir(parents=True, exist_ok=True)

    # Load and process data
    housing_data = load_and_process_housing_data()
    population_data = load_population_data()

    # Merge datasets
    merged_data = pd.merge(
        housing_data, population_data, on=["COUNTY_FIPS", "Year"], how="left"
    )

    # Save final output
    output_path = PATHS["processed"] / "cleaned_housing_data.csv"
    merged_data.to_csv(output_path, index=False)
    print(f"Data successfully saved to {output_path}")


if __name__ == "__main__":
    main()
