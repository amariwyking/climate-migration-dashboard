import pandas as pd
import os
from pathlib import Path
import sys
import warnings

warnings.filterwarnings('ignore', category=UserWarning)


def setup_directories():
    try:
        # Try assuming we're in project/scripts
        base_dir = Path.cwd().parent.parent / "data"
        if not base_dir.exists():
            # Fallback to current directory
            base_dir = Path.cwd() / "data"
        
        raw_dir = base_dir / "raw"
        
        # Job openings directories
        job_input_dir = raw_dir / "monthly_job_openings_xlsx_data"
        job_output_dir = raw_dir / "monthly_job_openings_csvs_data"
        
        # Public school directories
        school_input_dir = raw_dir / "public_school_xlsx_data"
        school_output_dir = raw_dir / "public_school_csvs_data"
        
        # Create output directories if they don't exist
        job_output_dir.mkdir(parents=True, exist_ok=True)
        school_output_dir.mkdir(parents=True, exist_ok=True)
        
        return job_input_dir, job_output_dir, school_input_dir, school_output_dir
    
    except Exception as e:
        print(f"Error setting up directories: {e}")
        raise

# -------------- Job Openings Processing Logic --------------

def extract_state_fips(series_id):
    """Extract the state FIPS code from the Series ID."""
    if len(series_id) >= 13:
        return series_id[9:11]
    return None

def process_job_openings_file(file_path):
    """Process a single job openings Excel file and return the state FIPS code and data."""
    try:
        # Read the Series ID line
        first_row = pd.read_excel(file_path, header=None)
        series_id_line = first_row.iloc[3, 1]

        # Validate Series ID format
        if not str(series_id_line).startswith("JTS"):
            print(f"Skipping file {file_path.name}: Invalid Series ID format")
            return None, None

        # Extract state FIPS code
        state_fips = extract_state_fips(series_id_line)
        if not state_fips:
            print(f"Skipping file {file_path.name}: Unable to extract FIPS code")
            return None, None

        # Read the data table
        df = pd.read_excel(file_path, skiprows=13)

        # Validate required columns
        required_columns = [
            "Year", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]
        if not set(required_columns).issubset(df.columns):
            print(f"Skipping file {file_path.name}: Missing required columns")
            return None, None

        print(f"Processed {file_path.name} - State FIPS: {state_fips}")
        return state_fips, df

    except Exception as e:
        print(f"Error processing {file_path.name}: {str(e)}")
        return None, None

def extract_yearly_data(state_fips, df):
    """Extract data by year from a dataframe."""
    yearly_data = {}

    for _, row in df.iterrows():
        if pd.isna(row["Year"]):
            continue

        year = int(row["Year"])

        # Extract monthly data
        monthly_columns = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]

        # Check if any monthly data is missing
        if any(pd.isna(row[month]) for month in monthly_columns):
            print(f"Skipping year {year} for state {state_fips}: incomplete monthly data")
            continue

        monthly_data = {month: row[month] for month in monthly_columns}

        # Initialize the year's data structure if not already present
        if year not in yearly_data:
            yearly_data[year] = {}

        # Add this state's data to the year
        yearly_data[year][state_fips] = monthly_data

    return yearly_data

def create_job_openings_csvs(yearly_data, output_dir):
    """Create CSV files for each year's job openings data."""
    for year, states_data in yearly_data.items():
        if not states_data:
            print(f"No data for year {year}, skipping CSV creation")
            continue

        # Create a DataFrame for this year
        df_data = []
        for state_fips, months in states_data.items():
            row_data = {"STATE": state_fips}
            row_data.update(months)
            df_data.append(row_data)

        # Create DataFrame from the collected data
        df_year = pd.DataFrame(df_data)

        # Set FIPS as index
        if "STATE" in df_year.columns:
            df_year.set_index("STATE", inplace=True)
            df_year.sort_index(inplace=True)

        # Save to CSV
        output_path = output_dir / f"state_job_opening_data_{year}.csv"
        df_year.to_csv(output_path)
        print(f"Saved {output_path}")

def process_job_openings(input_dir, output_dir):
    """Process all job openings Excel files and create CSV outputs."""
    if not input_dir.is_dir():
        print(f"Job openings input directory not found: {input_dir}")
        return False
    
    print(f"Processing job openings data from {input_dir}")
    yearly_data = {}

    # Process each Excel file
    for file_path in input_dir.glob("*.xlsx"):
        state_fips, df = process_job_openings_file(file_path)
        if state_fips and df is not None:
            # Extract and merge data into the yearly_data dictionary
            file_yearly_data = extract_yearly_data(state_fips, df)
            for year, states_data in file_yearly_data.items():
                if year not in yearly_data:
                    yearly_data[year] = {}
                yearly_data[year].update(states_data)

    # Create CSV files for each year
    create_job_openings_csvs(yearly_data, output_dir)
    return len(yearly_data) > 0

# -------------- Public School Processing Logic --------------

def consolidate_public_school_data(input_dir, output_dir):
    """Reads and consolidates all public school Excel files into a single CSV."""
    if not input_dir.is_dir():
        print(f"Public school input directory not found: {input_dir}")
        return False
    
    all_files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.endswith('.xls') or f.endswith('.xlsx')
    ]

    if not all_files:
        print(f"No Excel files found in {input_dir}")
        return False

    print(f"📥 Reading Excel files from: {input_dir}")

    dfs = []
    for file in all_files:
        try:
            df = pd.read_excel(file)
            dfs.append(df)
            print(f"Successfully read {os.path.basename(file)}")
        except Exception as e:
            print(f"❌ Failed to read {os.path.basename(file)}: {e}")

    if not dfs:
        print("No data was successfully processed from public school files")
        return False

    df_combined = pd.concat(dfs, ignore_index=True)
    
    # Determine the year - use 2023 as default or you could extract from filenames
    year = "2023"
    
    # Save to CSV
    output_path = output_dir / f"public_school_data_{year}.csv"
    df_combined.to_csv(output_path, index=False)
    
    print(f"\n:material/check_circle_outline: Consolidated public school DataFrame has {len(df_combined)} rows")
    print(f"Saved to {output_path}")
    
    return True

# -------------- Main Function --------------

def main():
    """Main function to run both conversion processes."""
    try:
        # Setup directories
        job_input_dir, job_output_dir, school_input_dir, school_output_dir = setup_directories()
        
        # Process job openings data
        job_success = False
        if job_input_dir.exists():
            job_success = process_job_openings(job_input_dir, job_output_dir)
            if job_success:
                print(":material/check_circle_outline: Job openings data processing completed successfully")
            else:
                print("⚠️ Job openings data processing completed with warnings")
        else:
            print(f"Job openings directory not found: {job_input_dir}")
        
        # Process public school data
        school_success = False
        if school_input_dir.exists():
            school_success = consolidate_public_school_data(school_input_dir, school_output_dir)
        else:
            print(f"Public school directory not found: {school_input_dir}")
        
        if job_success or school_success:
            print("Conversion process completed!")
        else:
            print("Conversion process completed but no data was processed")
            
    except Exception as e:
        print(f"❌ Error in main process: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())