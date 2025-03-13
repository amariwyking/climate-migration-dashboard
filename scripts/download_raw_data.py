from pathlib import Path
import pandas as pd
import censusdis.data as ced
from dotenv import load_dotenv
from typing import List
import os

load_dotenv()

# Constants
CONFIG = {
    "US_CENSUS_API_KEY": os.getenv("US_CENSUS_API_KEY"),
    "BASE_DATA_DIR": Path("../data/raw"),
    "HOUSING": {
        "DATASET": "acs/acs5/profile",
        "VARIABLES": {
            (2010, 2014): ["DP04_0001E", "DP04_0044E", "DP04_0088E", "DP04_0132E"],
            (2015, 2023): ["DP04_0001E", "DP04_0002E", "DP04_0089E", "DP04_0134E"],
        },
    },
    "POPULATION": {
        "DATASET": "acs/acs5",
        "VARIABLE": "B01003_001E",
        "EXCLUDED_STATES": ["District of Columbia", "Alaska", "Hawaii", "Puerto Rico"],
    },
    "AVAILABLE_YEARS": range(2010, 2024),
}


class CensusDataDownloader:
    def __init__(self):
        self._validate_api_key()

    def _validate_api_key(self):
        if not CONFIG["US_CENSUS_API_KEY"]:
            raise ValueError("US_CENSUS_API_KEY not found in .env file")

    @staticmethod
    def _create_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def _get_housing_variables(self, year: int) -> List[str]:
        for (start, end), variables in CONFIG["HOUSING"]["VARIABLES"].items():
            if start <= year <= end:
                return ["NAME"] + variables
        raise ValueError(f"No variables defined for year {year}")

    def download_housing_data(self) -> None:
        """Download housing data for all available years"""
        data_dir = CONFIG["BASE_DATA_DIR"] / "US_counties_housing_data_new"
        self._create_directory(data_dir)

        for year in CONFIG["AVAILABLE_YEARS"]:
            output_file = data_dir / f"census_housing_data_{year}.csv"
            if self._handle_existing_file(output_file, year):
                continue

            variables = self._get_housing_variables(year)
            self._download_dataset(
                dataset=CONFIG["HOUSING"]["DATASET"],
                year=year,
                variables=variables,
                output_file=output_file,
                description=f"housing data for {year}",
            )

    def download_population_data(self) -> None:
        """Download population data for all available years"""
        data_dir = CONFIG["BASE_DATA_DIR"] / "population_data"
        self._create_directory(data_dir)
        state_file = data_dir / "state_names.csv"

        contiguous_states = self._get_contiguous_states(state_file)

        for year in CONFIG["AVAILABLE_YEARS"]:
            output_file = data_dir / f"us_county_population_data_{year}.csv"
            if self._handle_existing_file(output_file, year):
                continue

            self._download_dataset(
                dataset=CONFIG["POPULATION"]["DATASET"],
                year=year,
                variables=["NAME", CONFIG["POPULATION"]["VARIABLE"]],
                output_file=output_file,
                description=f"population data for {year}",
                state=contiguous_states,
                county="*",
                with_geometry=True,
            )

    def _get_contiguous_states(self, state_file: Path) -> List[str]:
        """Get list of contiguous state codes"""
        if not state_file.exists():
            state_df = ced.download(
                CONFIG["POPULATION"]["DATASET"],
                2010,
                state="*",
                download_variables=["NAME"],
            )
            state_df.to_csv(state_file, index=False)
            print(f"State names saved to {state_file}")

        state_df = pd.read_csv(state_file)
        filtered = state_df[
            ~state_df["NAME"].isin(CONFIG["POPULATION"]["EXCLUDED_STATES"])
        ]
        return filtered["STATE"].astype(str).tolist()

    def _download_dataset(self, **kwargs) -> None:
        """Generic dataset download handler"""
        try:
            print(f"Downloading {kwargs['description']}...")
            df = ced.download(
                kwargs["dataset"],
                kwargs["year"],
                download_variables=kwargs["variables"],
                state=kwargs.get("state", "*"),
                county=kwargs.get("county", "*"),
                with_geometry=kwargs.get("with_geometry", False),
                api_key=CONFIG["US_CENSUS_API_KEY"],
            )
            self._save_data(df, kwargs["output_file"])
        except Exception as e:
            print(f"Error downloading {kwargs['description']}: {e}")

    @staticmethod
    def _handle_existing_file(output_file: Path, year: int) -> bool:
        """Check for existing files and handle appropriately"""
        if output_file.exists():
            print(
                f"Data file for {year} already exists at {output_file}. Skipping download."
            )
            return True
        return False

    @staticmethod
    def _save_data(df: pd.DataFrame, output_file: Path) -> None:
        """Save DataFrame to CSV with validation"""
        df.to_csv(output_file, index=False)
        print(
            f"Saved {len(df)} records with {len(df.columns)} variables to {output_file}"
        )


def main():
    downloader = CensusDataDownloader()
    downloader.download_housing_data()
    downloader.download_population_data()
    print("Download process completed.")


if __name__ == "__main__":
    main()
