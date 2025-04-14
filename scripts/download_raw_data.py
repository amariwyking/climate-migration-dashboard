from pathlib import Path
import pandas as pd
import censusdis.data as ced
import datacommons as dc
from dotenv import load_dotenv
from typing import List, Tuple, Dict
import os
import concurrent.futures
import time

load_dotenv()

# Constants
CONFIG = {
    "US_CENSUS_API_KEY": os.getenv("US_CENSUS_API_KEY"),
    "BASE_DATA_DIR": Path("./data/raw"),
    "EXCLUDED_STATES": [
        "11",
        "72",
        "15",
        "02",
        "78",
    ],  # DC, PR, HI, AK, VI as FIPS codes
    "DATASETS": {
        "HOUSING": {
            "DATASET": "acs/acs5/profile",
            "YEARS": (2010, 2023),
            "VARIABLES": {
                (2010, 2014): ["DP04_0001E", "DP04_0044E", "DP04_0088E", "DP04_0132E"],
                (2015, 2023): ["DP04_0001E", "DP04_0002E", "DP04_0089E", "DP04_0134E"],
            },
        },
        "POPULATION": {
            "DATASET": "acs/acs5",
            "YEARS": (2010, 2023),
            "VARIABLE": "B01003_001E",
        },
        "EDUCATION": {
            "DATASET": "acs/acs5",
            "YEARS": (2011, 2023),
            "VARIABLE": [
                "B23006_001E",
                "B23006_002E",
                "B23006_009E",
                "B23006_016E",
                "B23006_023E",
                "B14001_001E",
                "B14001_002E",
                "B14001_003E",
                "B14001_004E",
                "B14001_005E",
                "B14001_006E",
                "B14001_007E",
                "B14001_008E",
                "B14001_009E",
                "B23006_007E",
                "B23006_014E",
                "B23006_021E",
                "B23006_028E",
            ],
        },
        "ECONOMIC": {
            "DATASET": "acs/acs5",
            "YEARS": (2011, 2023),
            "VARIABLE": ["B19301_001E", "B23025_004E", "B23025_005E", "B23025_003E"],
        },
        "CRIME": {
            "DATA_SOURCE": "datacommons",
            "YEARS": (
                2010,
                2023,
            ),  # The range will be automatically filtered by available data
            "VARIABLES": ["Count_CriminalActivities_CombinedCrime"],
            "STATE_RANGE": (1, 80),  # Range of state IDs to try
        },
        "COUNTIES": {
            "DATASET": "acs/acs5",
            "YEARS": (2010, 2023),
            "VARIABLE": ["NAME"],
        },
    },
    "MAX_WORKERS": min(32, (os.cpu_count() or 1) + 4),  # Optimized concurrency
}


class DataDownloader:
    def __init__(self):
        self._validate_api_key()
        self.contiguous_states = self._get_contiguous_states()

    def _validate_api_key(self):
        if not CONFIG["US_CENSUS_API_KEY"]:
            raise ValueError("US_CENSUS_API_KEY not found in .env file")

    def _get_contiguous_states(self) -> List[str]:
        """Get list of contiguous state codes for all datasets"""
        state_data_dir = CONFIG["BASE_DATA_DIR"] / "state_data"
        state_data_dir.mkdir(parents=True, exist_ok=True)
        state_file = state_data_dir / "state_names.csv"

        if not state_file.exists():
            state_df = ced.download(
                "acs/acs5",
                2010,
                state="*",
                download_variables=["NAME"],
                api_key=CONFIG["US_CENSUS_API_KEY"],
            )
            # Filter out excluded states using FIPS codes
            state_df = state_df[
                ~state_df["STATE"].astype(str).isin(CONFIG["EXCLUDED_STATES"])
            ]
            state_df.to_csv(state_file, index=False)

        state_df = pd.read_csv(state_file)
        return state_df["STATE"].astype(str).str.zfill(2).tolist()

    @staticmethod
    def _get_years_from_range(year_range: Tuple[int, int]) -> List[int]:
        """Generate inclusive list of years from range tuple"""
        return list(range(year_range[0], year_range[1] + 1))

    def _get_variables_for_year(self, dataset: str, year: int) -> List[str]:
        """Dynamically get variables based on year and dataset with flexible configuration"""
        dataset_config = CONFIG["DATASETS"][dataset]

        # Check if dataset has a nested VARIABLES dictionary
        if "VARIABLES" in dataset_config:
            # Check if the variables are defined as a dictionary by year ranges
            if isinstance(dataset_config["VARIABLES"], dict):
                for (start, end), variables in dataset_config["VARIABLES"].items():
                    if start <= year <= end:
                        return ["NAME"] + variables
                raise ValueError(f"No variables defined for {dataset} in {year}")
            else:
                # Variables defined as a list directly
                return ["NAME"] + dataset_config["VARIABLES"]

        # Check if dataset has a single VARIABLE key (string or list)
        if "VARIABLE" in dataset_config:
            if isinstance(dataset_config["VARIABLE"], list):  # Multiple variables
                return ["NAME"] + dataset_config["VARIABLE"]
            elif isinstance(dataset_config["VARIABLE"], str):  # Single variable
                return ["NAME", dataset_config["VARIABLE"]]

        raise ValueError(f"Invalid variable configuration for {dataset}")

    def _download_single_dataset_year(self, dataset: str, year: int) -> None:
        """Download a single dataset for a specific year"""
        dataset_config = CONFIG["DATASETS"][dataset]

        # Skip if dataset is CRIME - it's handled separately
        if dataset == "CRIME":
            return

        data_dir = CONFIG["BASE_DATA_DIR"] / f"{dataset.lower()}_data"
        data_dir.mkdir(parents=True, exist_ok=True)

        output_file = data_dir / f"census_{dataset.lower()}_data_{year}.csv"
        if output_file.exists():
            print(f"Skipping existing {dataset} {year}")
            return

        try:
            variables = self._get_variables_for_year(dataset, year)
            print(f"Downloading {dataset} data for {year}...")

            df = ced.download(
                dataset_config["DATASET"],
                year,
                download_variables=variables,
                state=self.contiguous_states,
                county="*",
                with_geometry=("COUNTIES" in dataset),
                api_key=CONFIG["US_CENSUS_API_KEY"],
            )

            df.to_csv(output_file, index=False)
            print(f"Saved {dataset} {year} with {len(df)} records")

        except Exception as e:
            print(f"Failed {dataset} {year}: {str(e)}")

    def _download_dataset(self, dataset: str) -> None:
        """Parallel download handler for a dataset"""
        # Handle crime dataset separately
        if dataset == "CRIME":
            self._download_crime_data()
            return

        dataset_config = CONFIG["DATASETS"][dataset]
        years = self._get_years_from_range(dataset_config["YEARS"])

        # Use concurrent futures for parallel downloading
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONFIG["MAX_WORKERS"]
        ) as executor:
            # Create a list of futures for each year
            futures = [
                executor.submit(self._download_single_dataset_year, dataset, year)
                for year in years
            ]

            # Wait for all futures to complete
            concurrent.futures.wait(futures)

    def _download_crime_data(self) -> None:
        """Download crime data from Data Commons API"""
        crime_config = CONFIG["DATASETS"]["CRIME"]
        state_crime_output_dir = CONFIG["BASE_DATA_DIR"] / "state_crime_data"
        os.makedirs(state_crime_output_dir, exist_ok=True)

        # Get list of years within the specified range
        years_range = range(crime_config["YEARS"][0], crime_config["YEARS"][1] + 1)

        # Check which years already exist
        existing_files = {
            int(f.stem.split("_")[-1]): f
            for f in state_crime_output_dir.glob("state_crime_data_*.csv")
        }

        # Skip if all years exist
        if all(year in existing_files for year in years_range):
            print("All crime data files already exist, skipping download")
            return

        print("Downloading missing crime data from Data Commons...")

        # Create a dictionary to store data by year
        crime_data_by_year = {}

        # Collect data for all states
        state_range = crime_config["STATE_RANGE"]
        for state_id in range(state_range[0], state_range[1]):
            state_fips = f"{state_id:02d}"

            # Skip excluded states
            if state_fips in CONFIG["EXCLUDED_STATES"]:
                print(f"Skipping excluded state with FIPS: {state_fips}")
                continue

            try:
                print(f"Fetching crime data for (FIPS: {state_fips})...")
                data = dc.get_stat_series(
                    f"geoId/{state_fips}",
                    crime_config["VARIABLES"][
                        0
                    ],  # Using the first variable in the list
                )

                if data:  # Check if data exists
                    # For each year in the state's data
                    for year, value in data.items():
                        year_int = int(year)  # Convert year to integer for comparison

                        # Only process years that don't already exist and are within range
                        if (
                            crime_config["YEARS"][0]
                            <= year_int
                            <= crime_config["YEARS"][1]
                            and year_int not in existing_files
                        ):
                            if year not in crime_data_by_year:
                                crime_data_by_year[year] = []

                            crime_data_by_year[year].append(
                                {
                                    "STATE": state_fips,
                                    crime_config["VARIABLES"][0]: value,
                                }
                            )
            except Exception as e:
                print(f"Error fetching crime data for (FIPS: {state_fips}): {str(e)}")
                continue

        # Save separate CSV files for each year that doesn't already exist
        for year, data in crime_data_by_year.items():
            if data:
                df = pd.DataFrame(data)
                file_path = state_crime_output_dir / f"state_crime_data_{year}.csv"
                df.to_csv(file_path, index=False)
                print(f"Saved crime data for year {year} with {len(df)} records")

    def download_all_data(self):
        """Download all datasets with timing"""
        start_time = time.time()

        # Use concurrent futures for parallel dataset downloading
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONFIG["MAX_WORKERS"]
        ) as executor:
            # Create a list of futures for each dataset
            futures = [
                executor.submit(self._download_dataset, dataset)
                for dataset in CONFIG["DATASETS"]
            ]

            # Wait for all futures to complete
            concurrent.futures.wait(futures)

        end_time = time.time()
        print(f"Total download time: {end_time - start_time:.2f} seconds")


def main():
    downloader = DataDownloader()
    downloader.download_all_data()
    print("All dataset downloads completed.")


if __name__ == "__main__":
    main()
