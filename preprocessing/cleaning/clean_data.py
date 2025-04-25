from pathlib import Path
import pandas as pd
import re
import numpy as np
from typing import Dict

# Configuration constants
PATHS = {
    "processed": Path("./data/processed/cleaned_data"),
    "raw_data": {
        "economic": Path("./data/raw/economic_data"),
        "education": Path("./data/raw/education_data"),
        "housing": Path("./data/raw/housing_data"),
        "population": Path("./data/raw/population_data"),
        "counties": Path("./data/raw/counties_data"),
        "job_openings": Path("./data/raw/monthly_job_openings_csvs_data"),
        "crime": Path("./data/raw/state_crime_data"),
        "fema_nri": Path("./data/raw/county_fema_nri_data"),
        "cbsa": Path("./data/raw/cbsa_data"),
        "public_school": Path("./data/raw/public_school_csvs_data"),
    },
}

# Column Mappings
COLUMN_MAPPINGS = {
    "economic": {
        (2011, 2023): {
            "B19301_001E": "MEDIAN_INCOME",
            "B23025_004E": "TOTAL_EMPLOYED_POPULATION",
            "B23025_005E": "UNEMPLOYED_PERSONS",
            "B23025_003E": "TOTAL_LABOR_FORCE",
        }
    },
    "education": {
        (2011, 2023): {
            "B23006_001E": "TOTAL_POPULATION_25_64",
            "B23006_002E": "LESS_THAN_HIGH_SCHOOL_TOTAL",
            "B23006_009E": "HIGH_SCHOOL_GRADUATE_TOTAL",
            "B23006_016E": "SOME_COLLEGE_TOTAL",
            "B23006_023E": "BACHELORS_OR_HIGHER_TOTAL",
            "B14001_001E": "TOTAL_ENROLLED_AND_NOT_ENROLLED",
            "B14001_002E": "TOTAL_ENROLLED",
            "B14001_003E": "ENROLLED_NURSERY_PRESCHOOL",
            "B14001_004E": "ENROLLED_KINDERGARTEN",
            "B14001_005E": "ENROLLED_GRADE1_4",
            "B14001_006E": "ENROLLED_GRADE5_8",
            "B14001_007E": "ENROLLED_GRADE9_12",
            "B14001_008E": "ENROLLED_COLLEGE_UNDERGRAD",
            "B14001_009E": "ENROLLED_GRADUATE_PROFESSIONAL",
            "B23006_007E": "LESS_THAN_HIGH_SCHOOL_UNEMPLOYED",
            "B23006_014E": "HIGH_SCHOOL_GRADUATE_UNEMPLOYED",
            "B23006_021E": "SOME_COLLEGE_UNEMPLOYED",
            "B23006_028E": "BACHELORS_OR_HIGHER_UNEMPLOYED",
            "B01001_004E": "MALE_5-9", 
            "B01001_005E": "MALE_10-14",
            "B01001_006E": "MALE_15-17",
            "B01001_028E": "FEMALE_5-9",
            "B01001_029E": "FEMALE_10-14",
            "B01001_030E": "FEMALE_15-17" 
        }
    },
    "housing": {
        (2010, 2014): {
            "DP04_0001E": "TOTAL_HOUSING_UNITS",
            "DP04_0044E": "OCCUPIED_HOUSING_UNITS",
            "DP04_0088E": "MEDIAN_HOUSING_VALUE",
            "DP04_0132E": "MEDIAN_GROSS_RENT",
        },
        (2015, 2023): {
            "DP04_0001E": "TOTAL_HOUSING_UNITS",
            "DP04_0002E": "OCCUPIED_HOUSING_UNITS",
            "DP04_0089E": "MEDIAN_HOUSING_VALUE",
            "DP04_0134E": "MEDIAN_GROSS_RENT",
        },
    },
    "job_openings": {
        (2010, 2023): {
            "Jan": "JOB_OPENING_JAN",
            "Feb": "JOB_OPENING_FEB",
            "Apr": "JOB_OPENING_APR",
            "May": "JOB_OPENING_MAY",
            "Mar": "JOB_OPENING_MAR",
            "Jun": "JOB_OPENING_JUN",
            "Jul": "JOB_OPENING_JUL",
            "Aug": "JOB_OPENING_AUG",
            "Sep": "JOB_OPENING_SEP",
            "Oct": "JOB_OPENING_OCT",
            "Nov": "JOB_OPENING_NOV",
            "Dec": "JOB_OPENING_DEC",
        }
    },
    "crime": {
        (2010, 2023): {
            "Count_CriminalActivities_CombinedCrime": "CRIMINAL_ACTIVITIES"
        }
    },
    "fema_nri": {
        (2021, 2023): {
            "FemaNaturalHazardRiskIndex_NaturalHazardImpact": "FEMA_NRI"
        }
    },
    "public_school": {
        (2022, 2023): {
          "Students": "PUBLIC_SCHOOL_STUDENTS",
          "Teachers": "PUBLIC_SCHOOL_TEACHERS",
        }
    },
}

POPULATION_COLUMN = "B01003_001E"
COMMON_COLUMNS = ["STATE", "COUNTY"]


class DataCleaner:
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

        # Remove YEAR and COUNTY_FIPS from z-score calculation
        z_score_cols = [
            col
            for col in numeric_cols
            if col not in ["YEAR", "COUNTY_FIPS", "POPULATION"]
        ]

        # Create a copy of the dataframe to avoid modifying the original
        df_with_z_scores = df.copy()

        # Calculate z-scores for each year and each numeric column
        for year in df["YEAR"].unique():
            year_mask = df["YEAR"] == year

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
        df["YEAR"] = year
        df = df.rename(columns={POPULATION_COLUMN: "POPULATION"})
        return df[["COUNTY_FIPS", "YEAR", "POPULATION", "STATE", "COUNTY", "NAME"]]

    @classmethod
    def load_county_population_data(cls) -> Dict[int, pd.DataFrame]:
        """Load county population data for job openings and crime data processing."""
        county_with_pop = {}
        years = range(2010, 2024)
        
        for year in years:
            try:
                pop_path = PATHS["raw_data"]["population"] / f"census_population_data_{year}.csv"
                if not pop_path.exists():
                    continue
                    
                pop_df = pd.read_csv(pop_path, dtype={'STATE': str, 'COUNTY': str})
                pop_df['STATE'] = pop_df['STATE'].str.zfill(2)
                pop_df['COUNTY'] = pop_df['COUNTY'].str.zfill(3)
                pop_df['COUNTY_FIPS'] = pop_df['STATE'] + pop_df['COUNTY']
                pop_df.rename(columns={POPULATION_COLUMN: 'POPULATION'}, inplace=True)
                
                # Select columns using a list
                county_with_pop[year] = pop_df[['COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME', 'POPULATION']]
            except Exception as e:
                print(f"Error loading population data for {year}: {e}")
                
        return county_with_pop
    
    @classmethod
    def cbsa_data(cls) -> pd.DataFrame:
        """Load core based statistical areas for US counties"""
        
        try:
            # Get the xlsx file
            pop_path = PATHS["raw_data"]["cbsa"] / f"cbsa_counties_data.xls"
            
            if not pop_path.exists():
                raise FileNotFoundError(f"Path '{pop_path}' does not exist")
                
            cbsa_df = pd.read_excel(pop_path, dtype={'FIPS State Code': str, 'FIPS County Code': str}, header=2)
            
            cbsa_df["COUNTY_FIPS"] = cbsa_df["FIPS State Code"].str.cat(cbsa_df["FIPS County Code"])
            cbsa_df["YEAR"] = 2023
            cbsa_df = cbsa_df[["COUNTY_FIPS", "CBSA Code", "Metropolitan/Micropolitan Statistical Area", "YEAR"]]
            
            # Filter for metropolitan areas only
            # cbsa_df = cbsa_df[cbsa_df["Metropolitan/Micropolitan Statistical Area"].eq("Metropolitan Statistical Area")]
            
            cbsa_df = cbsa_df.rename(columns={
                "CBSA Code": "CBSA",
                "Metropolitan/Micropolitan Statistical Area": "TYPE",
            })
            
            # cbsa_df = cbsa_df.set_index("COUNTY_FIPS")
            
            return cbsa_df
        except Exception as e:
            print(f"Error loading CBSA data")
            return e
            
        
    @classmethod
    def process_job_openings_data(cls) -> pd.DataFrame:
        """Process job openings data."""
        years = range(2010, 2024)
        all_county_data = []
        county_with_pop = cls.load_county_population_data()
        
        # Define monthly columns
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for year in years:
            try:
                job_path = PATHS["raw_data"]["job_openings"] / f"state_job_opening_data_{year}.csv"
                if not job_path.exists():
                    print(f"No job openings data found for {year}")
                    continue
                    
                job_openings = pd.read_csv(job_path, dtype={'STATE': str})
                job_openings['STATE'] = job_openings['STATE'].str.zfill(2)
                
                if year not in county_with_pop:
                    continue
                
                # Process county data
                county_data = county_with_pop[year].copy()
                
                # Calculate state-level population totals
                state_pop = county_data.groupby('STATE')['POPULATION'].sum().reset_index()
                state_pop.columns = ['STATE', 'STATE_POP']
                
                county_data = county_data.merge(state_pop, on='STATE', how='left')
                county_data['POP_RATIO'] = county_data['POPULATION'] / county_data['STATE_POP']
                
                # Merge and handle missing data
                county_data = county_data.merge(job_openings, left_on='STATE', right_on='STATE', how='left')
                
                # Calculate job openings for each month
                for month in months:
                    month_col = month
                    target_col = COLUMN_MAPPINGS["job_openings"][(2010, 2023)][month]
                    county_data[target_col] = round(county_data['POP_RATIO'] *
                                                  county_data[month_col].fillna(0) * 1000)
                
                # Select final columns
                final_columns = ['COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME', 'POPULATION'] + \
                                [COLUMN_MAPPINGS["job_openings"][(2010, 2023)][month] for month in months]
                cleaned_data = county_data[final_columns].dropna(how='all')
                cleaned_data['YEAR'] = year
                
                all_county_data.append(cleaned_data)
                
            except Exception as e:
                print(f"Error processing job openings data for {year}: {e}")
                continue
                
        # Combine all years into a single DataFrame
        if all_county_data:
            return pd.concat(all_county_data, ignore_index=True)
        return pd.DataFrame()

    @classmethod
    def process_crime_data(cls) -> pd.DataFrame:
        """Process crime data."""
        years = range(2010, 2024)
        all_county_data = []
        county_with_pop = cls.load_county_population_data()
        
        for year in years:
            try:
                crime_path = PATHS["raw_data"]["crime"] / f"state_crime_data_{year}.csv"
                if not crime_path.exists():
                    print(f"No crimes data found for {year}")
                    continue
                    
                crime_data = pd.read_csv(crime_path, dtype={'STATE': str})
                crime_data['STATE'] = crime_data['STATE'].str.zfill(2)
                
                if year not in county_with_pop:
                    continue
                
                # Process county data
                county_data = county_with_pop[year].copy()
                
                # Calculate state-level population totals
                state_pop = county_data.groupby('STATE')['POPULATION'].sum().reset_index()
                state_pop.columns = ['STATE', 'STATE_POP']
                
                county_data = county_data.merge(state_pop, on='STATE', how='left')
                county_data['POP_RATIO'] = county_data['POPULATION'] / county_data['STATE_POP']
                
                # Merge and handle missing data
                county_data = county_data.merge(crime_data, left_on='STATE', right_on='STATE', how='left')
                
                # Calculate criminal activities
                source_col = "Count_CriminalActivities_CombinedCrime"
                target_col = COLUMN_MAPPINGS["crime"][(2010, 2023)][source_col]
                county_data[target_col] = round(county_data['POP_RATIO'] * county_data[source_col].fillna(0))
                
                # Select final columns
                final_columns = ['COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME', 'POPULATION', target_col]
                cleaned_data = county_data[final_columns].dropna(how='all')
                cleaned_data['YEAR'] = year
                
                all_county_data.append(cleaned_data)
                
            except Exception as e:
                print(f"Error processing crime data for {year}: {e}")
                continue
                
        # Combine all years into a single DataFrame
        if all_county_data:
            return pd.concat(all_county_data, ignore_index=True)
        return pd.DataFrame()
    
    @classmethod
    def process_public_school_data(cls, data_type: str, year: int = 2023) -> pd.DataFrame:
        # Load county population data
        county_with_pop = cls.load_county_population_data()
        county_with_pop_year = county_with_pop[year].copy()
        county_with_pop_year['COUNTY NAME'] = county_with_pop_year['NAME'].str.split(',').str[0]
        
        # Define constants
        school_path = PATHS["raw_data"]["public_school"] / f"public_school_data_{year}.csv"
        columns_to_keep = ['County Name', 'State', 'Students', 'Teachers']
        state_to_fips = {
            "AL": "01", "AZ": "04", "AR": "05", "CA": "06",
            "CO": "08", "CT": "09", "DE": "10", "FL": "12",
            "GA": "13", "ID": "16", "IL": "17", "IN": "18",
            "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
            "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
            "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
            "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
            "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
            "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
            "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
            "WY": "56"
        }

        # Read and preprocess school data
        school_data = pd.read_csv(school_path, dtype={'County Name': str, 'State': str})
        school_data = school_data[columns_to_keep]
        
        # Convert numeric columns
        school_data['Students'] = pd.to_numeric(school_data['Students'], errors='coerce').fillna(0)
        school_data['Teachers'] = pd.to_numeric(school_data['Teachers'], errors='coerce').fillna(0)
        
        # Map state codes to FIPS
        school_data['State'] = school_data['State'].map(state_to_fips)
        
        # Rename columns according to mapping
        school_data = school_data.rename(columns=COLUMN_MAPPINGS["public_school"][(2022, 2023)])
        
        # Aggregate data by state and county
        school_data = school_data.groupby(['State', 'County Name']).agg({
            'PUBLIC_SCHOOL_STUDENTS': 'sum', 
            'PUBLIC_SCHOOL_TEACHERS': 'sum'
        }).reset_index()
        
        # Round numeric values
        school_data['PUBLIC_SCHOOL_STUDENTS'] = school_data['PUBLIC_SCHOOL_STUDENTS'].round()
        school_data['PUBLIC_SCHOOL_TEACHERS'] = school_data['PUBLIC_SCHOOL_TEACHERS'].round()
        school_data['STUDENT_TEACHER_RATIO'] = school_data['PUBLIC_SCHOOL_STUDENTS'] / school_data['PUBLIC_SCHOOL_TEACHERS']
        
        # Merge with population data
        school_data_with_pop = school_data.merge(
            county_with_pop_year, 
            left_on=['State', 'County Name'], 
            right_on=['STATE', 'COUNTY NAME'], 
            how='left'
        )
        
        # Clean up columns
        columns_from_county = ['COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME', 'POPULATION']
        school_data_with_pop = school_data_with_pop.dropna(subset=columns_from_county, how='all')
        school_data_with_pop.drop(columns=['County Name', 'State', 'COUNTY NAME'], inplace=True)
        school_data_with_pop['YEAR'] = year
        school_data_with_pop = school_data_with_pop.dropna(how='all')
        
        return school_data_with_pop


    @classmethod
    def process_and_save_data(cls, data_type: str):
        """Process and save a specific type of data."""
        # Create output directory
        PATHS["processed"].mkdir(parents=True, exist_ok=True)
        
        if data_type in ["economic", "education", "housing", "fema_nri"]:
            # Use existing method for these data types
            data = cls.load_and_process_data(data_type)
        elif data_type == "job_openings":
            data = cls.process_job_openings_data()
        elif data_type == "crime":
            data = cls.process_crime_data()
        elif data_type == "cbsa":
            data = cls.cbsa_data()
        elif data_type == "public_school":
            data = cls.process_public_school_data(data_type)
        else:
            print(f"Unknown data type: {data_type}")
            return
            
        # Load population data if not already included
        if 'POPULATION' not in data.columns and data_type not in ["job_openings", "crime"]:
            population_data = cls.load_population_data()
            # Merge datasets
            merged_data = pd.merge(
                data, population_data, on=["COUNTY_FIPS", "YEAR"], how="left"
            )
        else:
            merged_data = data

        # For housing data, add affordability metrics
        if data_type == "housing":
            # Load economic data to get MEDIAN_INCOME
            economic_data = cls.load_and_process_data("economic")
            economic_data = economic_data[["COUNTY_FIPS", "YEAR", "MEDIAN_INCOME"]]
            
            # Merge housing data with economic data
            merged_data = pd.merge(
                merged_data, 
                economic_data, 
                on=["COUNTY_FIPS", "YEAR"], 
                how="left"
            )
            
            # Calculate HOUSE_AFFORDABILITY
            merged_data["HOUSE_AFFORDABILITY"] = (
                (merged_data["MEDIAN_GROSS_RENT"] * 12) / merged_data["MEDIAN_INCOME"])

            merged_data.drop(columns=["MEDIAN_INCOME"], inplace=True)
            
        # Calculate z-scores
        merged_data_with_z_scores = cls.calculate_z_scores(merged_data)
        
        # Save final output
        output_path = PATHS["processed"] / f"cleaned_{data_type}_data.csv"
        merged_data_with_z_scores.to_csv(output_path, index=False)
        print(f"{data_type.capitalize()} data successfully saved to {output_path}")

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
            df = pd.read_csv(file, dtype=str, low_memory=False)
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
        processed_df["YEAR"] = year

        numeric_cols = list(column_map.values())
        processed_df[numeric_cols] = processed_df[numeric_cols].apply(
            pd.to_numeric, errors="coerce"
        )

        if data_type == "education":
            processed_df["ELEMENTARY_SCHOOL_POPULATION"] = processed_df["MALE_5-9"] + processed_df["FEMALE_5-9"]  # Ages 5–9
            processed_df["MIDDLE_SCHOOL_POPULATION"] = processed_df["MALE_10-14"] + processed_df["FEMALE_10-14"]  # Ages 10–14
            processed_df["HIGH_SCHOOL_POULATION"] = processed_df["MALE_15-17"] + processed_df["FEMALE_15-17"]  # Ages 15–17

        # Special processing for economic data
        if data_type == "economic":
            processed_df["UNEMPLOYMENT_RATE"] = (
                (processed_df["UNEMPLOYED_PERSONS"] / processed_df["TOTAL_LABOR_FORCE"])
                * 100
            ).round(2)

        return processed_df.drop(columns=COMMON_COLUMNS)

    @classmethod
    def clean_counties_data(cls):
        # Create output directory for counties data
        counties_output_dir = PATHS["processed"] / "counties_with_geometry"
        counties_output_dir.mkdir(parents=True, exist_ok=True)

        # Process each counties file by year
        for file in PATHS["raw_data"]["counties"].iterdir():
            if not file.is_file() or file.suffix != ".csv":
                continue

            year = cls.get_year_from_filename(file.name)
            if not year:
                continue

            # Read the counties data
            df = pd.read_csv(file, dtype={"STATE": str, "COUNTY": str})
            
            # Check if geometry column exists and rename to GEOMETRY
            if "geometry" in df.columns:
                df = df.rename(columns={"geometry": "GEOMETRY"})
            
            # Create COUNTY_FIPS by combining STATE and COUNTY columns
            if "STATE" in df.columns and "COUNTY" in df.columns:
                df["COUNTY_FIPS"] = (df["STATE"] + df["COUNTY"]).str.zfill(5)
            
            # Set COUNTY_FIPS as index
            df = df.set_index("COUNTY_FIPS")
            
            # Save the processed file with the same name
            output_path = counties_output_dir / file.name
            df.to_csv(output_path)
            
            print(f"Cleaned counties data for year {year} saved to {output_path}")


def main():
    # Process all types of data
    data_types = ["economic", "education", "housing", "job_openings", "crime", "fema_nri", "cbsa", "public_school"]

    # for data_type in data_types:
    for data_type in data_types:
        DataCleaner.process_and_save_data(data_type)
        
    # Process counties data separately by year
    DataCleaner.clean_counties_data()

    print("All data processing completed.")
    
    # DataCleaner.load_msa_data()


if __name__ == "__main__":
    main()