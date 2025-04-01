from pathlib import Path
import pandas as pd
import re
import numpy as np
from typing import Dict, List

# Configuration constants
PATHS = {
    "processed": Path("./data/preprocessed/cleaned_data"),
    "raw_data": {
        "economic": Path("./data/raw/economic_data"),
        "education": Path("./data/raw/education_data"),
        "housing": Path("./data/raw/housing_data"),
        "population": Path("./data/raw/population_data"),
    },
}

# Column Mappings
COLUMN_MAPPINGS = {
    "economic": {
        (2011, 2023): {
            "B19301_001E": "MEDIAN INCOME",
            "B23025_004E": "TOTAL EMPLOYED POPULATION",
            "B23025_005E": "UNEMPLOYED PERSONS",
            "B23025_003E": "TOTAL LABOR FORCE",
        }
    },
    "education": {
        (2011, 2023): {
            "B23006_001E": "TOTAL_POPULATION_25_64",
            "B23006_002E": "LESS_THAN_HIGH_SCHOOL_TOTAL",
            "B23006_009E": "HIGH_SCHOOL_GRADUATE_TOTAL",
            "B23006_016E": "SOME_COLLEGE_TOTAL",
            "B23006_023E": "BACHELOR_OR_HIGH_TOTAL",
            "B14001_001E": "TOTAL",
            "B14001_002E": "Enrolled",
            "B14001_003E": "ENROLLED_NURSERY_PRESCOOL",
            "B14001_004E": "ENROLLED_KINDERGARTEN",
            "B14001_005E": "ENROLLED_GRADE1_4",
            "B14001_006E": "ENROLLED_GRADE5_8",
            "B14001_007E": "ENROLLED_GRADE9_12",
            "B14001_008E": "ENROLLED_COLLEGE_UNDERGRAD",
            "B14001_009E": "ENROLLED_GRADUATE_PROFESSIONAL",
            "B23006_007E": "LESS_THAN_HIGH_SCHOOL_UNEMPLOYED",
            "B23006_014E": "HIGH_SCHOOL_GRADUATE_UNEMPLOYED",
            "B23006_021E": "SOME_COLLEGE_UNEMLOYED",
            "B23006_028E": "BACHELOR_OR_HIGH_UNEMPLOYED",
        }
    },
    "housing": {
        (2010, 2014): {
            "DP04_0001E": "TOTAL_HOUSING_UNITS",
            "DP04_0044E": "OCCUPIED_HOUSING_UNTIS",
            "DP04_0088E": "MEDIAN_HOUSING_VALUE",
            "DP04_0132E": "MEDIAN_GROSS_RENT",
        },
        (2015, 2023): {
            "DP04_0001E": "TOTAL_HOUSING_UNITS",
            "DP04_0002E": "OCCUPIED_HOUSING_UNTIS",
            "DP04_0089E": "MEDIAN_HOUSING_VALUE",
            "DP04_0134E": "MEDIAN_GROSS_RENT",
        },
    },
}

POPULATION_COLUMN = "B01003_001E"
COMMON_COLUMNS = ["STATE", "COUNTY"]


class CensusDataCleaner:
    @staticmethod
    def get_year_from_filename(filename: str) -> int:
        """Extract year from filename using regex pattern."""
        match = re.search(r"(\d{4})", filename)
        return int(match.group(1)) if match else None

    @classmethod
    def calculate_z_scores(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate z-scores for numeric columns.

        Z-score represents how many standard deviations an observation is from the mean.
        This calculation is done across all counties for each numeric column.
        """
        # Identify numeric columns (excluding non-numeric columns)
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        # Remove Year and COUNTY_FIPS from z-score calculation
        z_score_cols = [
            col
            for col in numeric_cols
            if col not in ["Year", "COUNTY_FIPS", "POPULATION"]
        ]

        # Create a copy of the dataframe to avoid modifying the original
        df_with_z_scores = df.copy()

        # Calculate z-scores for each year and each numeric column
        for year in df["Year"].unique():
            year_mask = df["Year"] == year

            for col in z_score_cols:
                # Create z-score column name
                z_score_col = f"{col}_Z_SCORE"

                # Calculate z-score for the specific year
                year_data = df.loc[year_mask, col]
                df_with_z_scores.loc[year_mask, z_score_col] = (
                    (year_data - year_data.mean()) / year_data.std()
                ).round(4)

        return df_with_z_scores

    @classmethod
    def load_and_process_data(cls, data_type: str) -> pd.DataFrame:
        """Load and process data from raw CSV files."""
        all_dfs = []

        for file in PATHS["raw_data"][data_type].iterdir():
            if not file.is_file() or file.suffix != ".csv":
                continue

            year = cls.get_year_from_filename(file.name)
            if not year:
                continue

            # Select appropriate column mapping
            column_map = next(
                (
                    mapping
                    for (start, end), mapping in COLUMN_MAPPINGS[data_type].items()
                    if start <= year <= end
                ),
                None,
            )
            if not column_map:
                continue

            # Read and process data
            df = pd.read_csv(file, skiprows=[1], dtype=str, low_memory=False)
            df = cls.process_dataframe(df, column_map, year, data_type)
            all_dfs.append(df)

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    @classmethod
    def process_dataframe(
        cls, df: pd.DataFrame, column_map: Dict[str, str], year: int, data_type: str
    ) -> pd.DataFrame:
        """Process individual dataframe."""
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

        # Special processing for economic data
        if data_type == "economic":
            processed_df["UNEMPLOYMENT RATE"] = (
                (processed_df["UNEMPLOYED PERSONS"] / processed_df["TOTAL LABOR FORCE"])
                * 100
            ).round(2)

        return processed_df.drop(columns=COMMON_COLUMNS)

    @classmethod
    def load_population_data(cls) -> pd.DataFrame:
        """Load and process population data from raw CSV files."""
        pop_frames = []

        for file in PATHS["raw_data"]["population"].iterdir():
            if not file.is_file():
                continue

            year = cls.get_year_from_filename(file.name)
            if not year:
                continue

            df = pd.read_csv(file, dtype=str, low_memory=False)
            processed_df = cls.process_population_dataframe(df, year)
            pop_frames.append(processed_df)

        return (
            pd.concat(pop_frames, ignore_index=True) if pop_frames else pd.DataFrame()
        )

    @staticmethod
    def process_population_dataframe(df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Process individual population dataframe."""
        df["COUNTY_FIPS"] = (df["STATE"] + df["COUNTY"]).str.zfill(5)
        df["Year"] = year
        return df.rename(columns={POPULATION_COLUMN: "POPULATION"})[
            ["COUNTY_FIPS", "Year", "POPULATION"]
        ]

    @classmethod
    def process_and_save_data(cls, data_type: str):
        """Process and save a specific type of data."""
        # Create output directory
        PATHS["processed"].mkdir(parents=True, exist_ok=True)

        # Load and process data
        data = cls.load_and_process_data(data_type)
        population_data = cls.load_population_data()

        # Merge datasets
        merged_data = pd.merge(
            data, population_data, on=["COUNTY_FIPS", "Year"], how="left"
        )

        # Calculate z-scores
        merged_data_with_z_scores = cls.calculate_z_scores(merged_data)

        # Save final output
        output_path = PATHS["processed"] / f"cleaned_{data_type}_data.csv"
        merged_data_with_z_scores.to_csv(output_path, index=False)
        print(f"{data_type.capitalize()} data successfully saved to {output_path}")


def main():
    # Process all types of data
    data_types = ["economic", "education", "housing"]

    for data_type in data_types:
        CensusDataCleaner.process_and_save_data(data_type)

    print("All data processing completed.")


if __name__ == "__main__":
    main()
