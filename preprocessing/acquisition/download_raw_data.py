from pathlib import Path
import pandas as pd
import censusdis.data as ced
import datacommons as dc
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Optional
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
                "B01001_004E",  # Male 5-9
                "B01001_005E",  # Male 10-14
                "B01001_006E",  # Male 15-17
                "B01001_028E",  # Female 5-9
                "B01001_029E",  # Female 10-14
                "B01001_030E"   # Female 15-17
            ],
        },
        "ECONOMIC": {
            "DATASET": "acs/acs5",
            "YEARS": (2011, 2023),
            "VARIABLE": ["B19301_001E", "B23025_004E", "B23025_005E", "B23025_003E"],
        },
        "CRIME": {
            "DATA_SOURCE": "datacommons",
            "YEARS": (2010, 2023),
            "VARIABLES": ["Count_CriminalActivities_CombinedCrime"],
            "STATE_RANGE": (1, 80),  # Range of state IDs to try
            "LEVEL": "state",  # Indicates state-level data
        },
        "FEMA_NRI": {
            "DATA_SOURCE": "datacommons",
            "YEARS": (2021, 2023),
            "VARIABLES": ["FemaNaturalHazardRiskIndex_NaturalHazardImpact"],
            "LEVEL": "county",  # Indicates county-level data
        },
        "COUNTIES": {
            "DATASET": "acs/acs5",
            "YEARS": (2010, 2023),
            "VARIABLE": ["NAME"],
        },
    },
    "MAX_WORKERS": min(32, (os.cpu_count() or 1) + 4),  # Optimized concurrency
    "MAX_COUNTY_WORKERS": min(50, (os.cpu_count() or 1) * 2),  # Specific for county downloads
}


class DataDownloader:
    def __init__(self):
        self._validate_api_key()
        self.contiguous_states = self._get_contiguous_states()
        self.counties_by_state = self._get_counties_by_state()

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
    
    def _get_counties_by_state(self) -> Dict[str, List[str]]:
        """Get a mapping of state codes to their county codes"""
        counties_data_dir = CONFIG["BASE_DATA_DIR"] / "county_data"
        counties_data_dir.mkdir(parents=True, exist_ok=True)
        counties_file = counties_data_dir / "county_names.csv"
        
        if not counties_file.exists():
            # Download county data
            counties_df = ced.download(
                "acs/acs5",
                2010,
                state=self.contiguous_states,
                county="*",
                download_variables=["NAME"],
                api_key=CONFIG["US_CENSUS_API_KEY"],
            )
            counties_df.to_csv(counties_file, index=False)
        
        counties_df = pd.read_csv(counties_file)
        
        # Create dictionary mapping state to county codes
        counties_by_state = {}
        for state in self.contiguous_states:
            state_counties = counties_df[counties_df["STATE"] == int(state)]["COUNTY"].astype(str).str.zfill(3).tolist()
            counties_by_state[state] = state_counties
            
        return counties_by_state

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

        # Skip if dataset is from Data Commons - it's handled separately
        if dataset_config.get("DATA_SOURCE") == "datacommons":
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
        dataset_config = CONFIG["DATASETS"][dataset]
        
        # Handle Data Commons datasets
        if dataset_config.get("DATA_SOURCE") == "datacommons":
            self._download_datacommons_dataset(dataset)
            return

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

    def _download_datacommons_dataset(self, dataset: str) -> None:
        """Generalized method to download data from Data Commons API"""
        dataset_config = CONFIG["DATASETS"][dataset]
        
        # Determine the level (state or county) and set the appropriate subdirectory name
        level = dataset_config.get("LEVEL", "state")  # Default to state if not specified
        
        # Set up output directory
        output_dir = CONFIG["BASE_DATA_DIR"] / f"{level}_{dataset.lower()}_data"
        os.makedirs(output_dir, exist_ok=True)
        
        # Get years range
        years_range = range(dataset_config["YEARS"][0], dataset_config["YEARS"][1] + 1)
        
        # Check which years already exist
        existing_files = {
            int(f.stem.split("_")[-1]): f
            for f in output_dir.glob(f"{level}_{dataset.lower()}_data_*.csv")
        }
        
        # Skip if all years exist
        if all(year in existing_files for year in years_range):
            print(f"All {dataset} data files already exist, skipping download")
            return
        
        print(f"Downloading missing {dataset} data from Data Commons...")
        
        # Create a dictionary to store all data (global to all threads)
        all_data = {year: [] for year in years_range if year not in existing_files}
        
        # Process based on level (state or county)
        if level == "state":
            # State level data - use state_range if available
            state_range = dataset_config.get("STATE_RANGE", (1, 80))  # Default state range
            
            # Create a list of state IDs to process
            state_ids = [
                f"{state_id:02d}" 
                for state_id in range(state_range[0], state_range[1])
                if f"{state_id:02d}" not in CONFIG["EXCLUDED_STATES"]
            ]
            
            # Process states in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor:
                futures = []
                for state_fips in state_ids:
                    futures.append(
                        executor.submit(
                            self._fetch_datacommons_for_geo,
                            geo_id=f"geoId/{state_fips}",
                            variables=dataset_config["VARIABLES"],
                            all_data=all_data,
                            years_range=years_range,
                            existing_files=existing_files,
                            state=state_fips
                        )
                    )
                
                # Wait for all futures to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Error in thread: {str(e)}")
                        
        elif level == "county":
            # County level data - create a list of all county tasks
            county_tasks = []
            
            for state_fips in self.contiguous_states:
                for county_fips in self.counties_by_state.get(state_fips, []):
                    county_tasks.append((state_fips, county_fips))
            
            # Process counties in parallel using a thread pool
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG["MAX_COUNTY_WORKERS"]) as executor:
                futures = []
                
                for state_fips, county_fips in county_tasks:
                    geo_id = f"geoId/{state_fips}{county_fips}"
                    futures.append(
                        executor.submit(
                            self._fetch_datacommons_for_geo,
                            geo_id=geo_id,
                            variables=dataset_config["VARIABLES"],
                            all_data=all_data,
                            years_range=years_range,
                            existing_files=existing_files,
                            state=state_fips,
                            county=county_fips
                        )
                    )
                
                # Use a progress counter
                completed = 0
                total = len(futures)
                
                # Wait for all futures to complete while showing progress
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                        completed += 1
                        if completed % 50 == 0 or completed == total:
                            print(f"Progress: {completed}/{total} counties processed ({completed/total*100:.1f}%)")
                    except Exception as e:
                        print(f"Error in county download thread: {str(e)}")
                        completed += 1
        
        # Save separate CSV files for each year
        for year, data in all_data.items():
            if data:
                df = pd.DataFrame(data)
                file_path = output_dir / f"{level}_{dataset.lower()}_data_{year}.csv"
                df.to_csv(file_path, index=False)
                print(f"Saved {dataset} data for year {year} with {len(df)} records")
    
    def _fetch_datacommons_for_geo(
        self, 
        geo_id: str, 
        variables: List[str], 
        all_data: Dict,
        years_range: range,
        existing_files: Dict,
        state: str,
        county: Optional[str] = None
    ) -> None:
        """Fetch Data Commons data for a specific geography"""
        try:
            geo_name = f"FIPS: {state}" if county is None else f"FIPS: {state}{county}"
            
            # Only print for state-level data to reduce console noise
            if county is None:
                print(f"Fetching data for {geo_name}...")
            
            # Process variables in sequence (Data Commons API might have rate limits)
            for variable in variables:
                data = dc.get_stat_series(geo_id, variable) 
                
                if data:  # Check if data exists
                    # For each year in the data
                    for year, value in data.items():
                        if len(year) != 4:
                            year = year[:4]  # Ensure year is 4 digits
                        year_int = int(year)  # Convert year to integer for comparison
                        
                        # For debugging
                        if county is None:  # Only print for state-level to reduce noise
                            print(f"Processing year {year_int} for {geo_id}, variable {variable}")
                        
                        # Only process years that are within range and don't already exist in files
                        if (
                            year_int in years_range
                            and year_int not in existing_files
                        ):
                            entry = {
                                "STATE": state,
                                variable: value,
                            }
                            
                            # Add county if applicable
                            if county:
                                entry["COUNTY"] = county
                                
                            # Thread-safe append to the data dictionary - use year_int directly
                            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as lock_executor:
                                lock_executor.submit(lambda: all_data[year_int].append(entry))
        except Exception as e:
            if county is None:
                print(f"Error fetching data for {geo_id}: {str(e)}")
            # For county-level errors, only log if it's a significant error (to reduce noise)
            elif "404" not in str(e):  # Ignore common 404 errors for counties
                print(f"Error for {geo_id}: {str(e)[:100]}...")

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