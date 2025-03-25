from pathlib import Path
import pandas as pd
import censusdis.data as ced
from dotenv import load_dotenv
from typing import List, Tuple
import os
import concurrent.futures
import time

load_dotenv()

# Constants
CONFIG = {
    "US_CENSUS_API_KEY": os.getenv("US_CENSUS_API_KEY"),
    "BASE_DATA_DIR": Path("./data/raw"),
    "EXCLUDED_STATES": ["District of Columbia", "Alaska", "Hawaii", "Puerto Rico"],
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
            "VARIABLES": {
                (2011, 2023): [
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
                ]
            },
        },
    },
    "MAX_WORKERS": min(32, (os.cpu_count() or 1) + 4),  # Optimized concurrency
}


class CensusDataDownloader:
    def __init__(self):
        self._validate_api_key()
        self.contiguous_states = self._get_contiguous_states()

    def _validate_api_key(self):
        if not CONFIG["US_CENSUS_API_KEY"]:
            raise ValueError("US_CENSUS_API_KEY not found in .env file")

    def _get_contiguous_states(self) -> List[str]:
        """Get list of contiguous state codes for all datasets"""
        metadata_dir = CONFIG["BASE_DATA_DIR"] / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        state_file = metadata_dir / "state_names.csv"

        if not state_file.exists():
            state_df = ced.download(
                "acs/acs5",
                2010,
                state="*",
                download_variables=["NAME"],
                api_key=CONFIG["US_CENSUS_API_KEY"],
            )
            state_df = state_df[~state_df["NAME"].isin(CONFIG["EXCLUDED_STATES"])]
            state_df.to_csv(state_file, index=False)

        state_df = pd.read_csv(state_file)
        return state_df["STATE"].astype(str).str.zfill(2).tolist()

    @staticmethod
    def _get_years_from_range(year_range: Tuple[int, int]) -> List[int]:
        """Generate inclusive list of years from range tuple"""
        return list(range(year_range[0], year_range[1] + 1))

    def _get_variables_for_year(self, dataset: str, year: int) -> List[str]:
        """Dynamically get variables based on year and dataset"""
        dataset_config = CONFIG["DATASETS"][dataset]

        if "VARIABLE" in dataset_config:  # Single variable datasets
            return ["NAME", dataset_config["VARIABLE"]]

        for (start, end), variables in dataset_config["VARIABLES"].items():
            if start <= year <= end:
                return ["NAME"] + variables
        raise ValueError(f"No variables defined for {dataset} in {year}")

    def _download_single_dataset_year(self, dataset: str, year: int) -> None:
        """Download a single dataset for a specific year"""
        dataset_config = CONFIG["DATASETS"][dataset]
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
                with_geometry=("POPULATION" in dataset),
                api_key=CONFIG["US_CENSUS_API_KEY"],
            )

            df.to_csv(output_file, index=False)
            print(f"Saved {dataset} {year} with {len(df)} records")

        except Exception as e:
            print(f"Failed {dataset} {year}: {str(e)}")

    def _download_dataset(self, dataset: str) -> None:
        """Parallel download handler for a dataset"""
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
    downloader = CensusDataDownloader()
    downloader.download_all_data()
    print("All dataset downloads completed.")


if __name__ == "__main__":
    main()
