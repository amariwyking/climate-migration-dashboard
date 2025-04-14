import pandas as pd
from pathlib import Path


def setup_directories():
    """Initialize input and output directories."""
    input_dir = Path("./data/raw/monthly_job_openings_xlsx_data")
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Directory not found: {input_dir}")
    output_dir = Path("./data/raw/monthly_job_openings_csvs_data")
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


def extract_state_fips(series_id):
    """Extract the state FIPS code from the Series ID."""
    if len(series_id) >= 13:
        return series_id[9:11]
    return None


def process_excel_file(file_path):
    """Process a single Excel file and return the state FIPS code and data."""
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
            "Year",
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
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
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]

        # Check if any monthly data is missing
        if any(pd.isna(row[month]) for month in monthly_columns):
            print(
                f"Skipping year {year} for state {state_fips}: incomplete monthly data"
            )
            continue

        monthly_data = {month: row[month] for month in monthly_columns}

        # Initialize the year's data structure if not already present
        if year not in yearly_data:
            yearly_data[year] = {}

        # Add this state's data to the year
        yearly_data[year][state_fips] = monthly_data

    return yearly_data


def create_yearly_csvs(yearly_data, output_dir):
    """Create CSV files for each year's data."""
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


def main():
    """Main function to process all Excel files and create CSV outputs."""
    input_dir, output_dir = setup_directories()
    yearly_data = {}

    # Process each Excel file
    for file_path in input_dir.glob("*.xlsx"):
        state_fips, df = process_excel_file(file_path)
        if state_fips and df is not None:
            # Extract and merge data into the yearly_data dictionary
            file_yearly_data = extract_yearly_data(state_fips, df)
            for year, states_data in file_yearly_data.items():
                if year not in yearly_data:
                    yearly_data[year] = {}
                yearly_data[year].update(states_data)

    # Create CSV files for each year
    create_yearly_csvs(yearly_data, output_dir)
    print("Conversion complete!")


if __name__ == "__main__":
    main()
