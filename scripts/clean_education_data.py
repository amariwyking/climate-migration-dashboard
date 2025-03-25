from pathlib import Path
import pandas as pd
import re
from typing import Dict

# Configuration constants
PATHS = {
    "processed": Path("./data/processed/cleaned_data"),
    "raw_education": Path("./data/raw/education_data"),
    "raw_population": Path("./data/raw/population_data"),
}

EDUCATION_DATA_COLUMN_MAPPINGS = {
    (2011, 2023): {
        "B23006_001E": "TOTAL_POPULATION_25_64",
        "B23006_002E": "LESS_THAN_HIGH_SCHOOL_TOTAL",
        "B23006_009E": "HIGH_SCHOOL_GRADUATE_TOTAL",
        "B23006_016E": "SOME_COLLEGE_TOTAL",
        "B23006_023E": "BACHELOR_OR_HIGH_TOTAL",
    }
}

COMMON_COLUMNS = ["STATE", "COUNTY"]
POPULATION_COLUMN = "B01003_001E"


def get_year_from_filename(filename: str) -> int:
    """Extract year from filename using regex pattern."""
    match = re.search(r"(\d{4})", filename)
    return int(match.group(1)) if match else None


def load_and_process_education_data() -> pd.DataFrame:
    """Load and process education data from raw CSV files."""
    all_dfs = []

    for file in PATHS["raw_education"].iterdir():
        if not file.is_file() or file.suffix != ".csv":
            continue

        year = get_year_from_filename(file.name)
        if not year:
            continue

        # Select appropriate column mapping
        column_map = next(
            (
                mapping
                for (start, end), mapping in EDUCATION_DATA_COLUMN_MAPPINGS.items()
                if start <= year <= end
            ),
            None,
        )
        if not column_map:
            continue

        # Read and process data
        df = pd.read_csv(file, skiprows=[1], dtype=str, low_memory=False)
        df = process_education_dataframe(df, column_map, year)
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


def process_education_dataframe(
    df: pd.DataFrame, column_map: Dict[str, str], year: int
) -> pd.DataFrame:
    """Process individual education dataframe."""
    columns = list(column_map.keys()) + COMMON_COLUMNS
    processed_df = df[columns].rename(columns=column_map)

    processed_df["COUNTY_FIPS"] = (
        processed_df["STATE"] + processed_df["COUNTY"]
    ).str.zfill(5)
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
        if not file.is_file():
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
    df["COUNTY_FIPS"] = (df["STATE"] + df["COUNTY"]).str.zfill(5)
    df["Year"] = year
    return df.rename(columns={POPULATION_COLUMN: "POPULATION"})[
        ["COUNTY_FIPS", "Year", "POPULATION"]
    ]


def main():
    # Create output directory
    PATHS["processed"].mkdir(parents=True, exist_ok=True)

    # Load and process data
    education_data = load_and_process_education_data()
    population_data = load_population_data()

    # Merge datasets
    merged_data = pd.merge(
        education_data, population_data, on=["COUNTY_FIPS", "Year"], how="left"
    )

    # Save final output
    output_path = PATHS["processed"] / "cleaned_education_data.csv"
    merged_data.to_csv(output_path, index=False)
    print(f"Data successfully saved to {output_path}")


if __name__ == "__main__":
    main()
