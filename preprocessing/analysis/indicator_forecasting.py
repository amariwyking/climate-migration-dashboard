import pandas as pd
from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.discriminant_analysis import StandardScaler

# Define directory paths
DATA_DIR = Path("./data")
PROCESSED_DIR = DATA_DIR / "processed"
RAW_DATA_DIR = DATA_DIR / "raw"
POPULATION_DIR = RAW_DATA_DIR / "population_data"
CLEANED_DIR = PROCESSED_DIR / "cleaned_data"
PROJECTED_DATA = PROCESSED_DIR / "projected_data"

# Define file paths
CRIME_DATA = CLEANED_DIR / "cleaned_crime_data.csv"
ECONOMIC_DATA = CLEANED_DIR / "cleaned_economic_data.csv"
EDUCATION_DATA = CLEANED_DIR / "cleaned_education_data.csv"
HOUSING_DATA = CLEANED_DIR / "cleaned_housing_data.csv"
JOB_OPENINGS_DATA = CLEANED_DIR / "cleaned_job_openings_data.csv"
STUDENT_TEACHER_DATA = CLEANED_DIR / "erie_student_teacher.csv"
POP_PROJECT = PROJECTED_DATA / "county_population_projections.csv"
POP_2023 = POPULATION_DIR / "census_population_data_2023.csv"
PUBLIC_SCHOOL_DATA = CLEANED_DIR / "cleaned_public_school_data.csv"

def load_and_merge_data():
    """Load and merge all datasets into a single dataframe"""
    # Load individual datasets
    economic_df = pd.read_csv(ECONOMIC_DATA)
    education_df = pd.read_csv(EDUCATION_DATA)
    housing_df = pd.read_csv(HOUSING_DATA)
    job_openings_df = pd.read_csv(JOB_OPENINGS_DATA)
    public_school_df = pd.read_csv(PUBLIC_SCHOOL_DATA)
    
    # Merge all dataframes on COUNTY_FIPS
    merged_df = economic_df.merge(
        education_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'YEAR'], how='inner'
    ).merge(
        housing_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'YEAR'], how='inner'
    ).merge(
        job_openings_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'YEAR'], how='inner'
    ).merge(
        public_school_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'YEAR'], how='outer'
    )
    
    # Drop columns containing 'z_score'
    merged_df = merged_df.loc[:, ~merged_df.columns.str.contains('z_score', case=False)]
    
    # Set school data to 0 for years other than 2023
    merged_df.loc[merged_df["YEAR"] != 2023, ["PUBLIC_SCHOOL_STUDENTS", "PUBLIC_SCHOOL_TEACHERS", "STUDENT_TEACHER_RATIO"]] = 0
    
    return merged_df

def prepare_filtered_data(merged_df):
    """Prepare filtered data for 2023"""
    filter_columns = [
        'PUBLIC_SCHOOL_STUDENTS', 'ELEMENTARY_SCHOOL_POPULATION', 
        'MIDDLE_SCHOOL_POPULATION', 'HIGH_SCHOOL_POULATION', 
        'COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME', 
        'TOTAL_EMPLOYED_POPULATION', 'TOTAL_LABOR_FORCE',
        'JOB_OPENING_JAN', 'JOB_OPENING_FEB', 'JOB_OPENING_MAR', 
        'JOB_OPENING_APR', 'JOB_OPENING_MAY', 'JOB_OPENING_JUN', 
        'JOB_OPENING_JUL', 'JOB_OPENING_AUG', 'JOB_OPENING_SEP', 
        'JOB_OPENING_OCT', 'JOB_OPENING_NOV', 'JOB_OPENING_DEC',
        'POPULATION', 'YEAR', 'OCCUPIED_HOUSING_UNITS'
    ]
    
    filtered_df = merged_df[filter_columns]
    filtered_df = filtered_df[filtered_df["YEAR"] == 2023]
    filtered_df['COUNTY_FIPS'] = filtered_df['COUNTY_FIPS'].astype(str).str.zfill(5)
    
    return filtered_df

def process_population_data():
    """Process population data and calculate percentage changes"""
    pop_project_df = pd.read_csv(POP_PROJECT)
    pop_2023 = pd.read_csv(POP_2023)
    
    # Format county FIPS codes
    pop_2023["STATE"] = pop_2023["STATE"].astype(str).str.zfill(2)
    pop_2023["COUNTY"] = pop_2023["COUNTY"].astype(str).str.zfill(3)
    pop_2023["COUNTY_FIPS"] = pop_2023["STATE"] + pop_2023["COUNTY"]
    pop_project_df["COUNTY_FIPS"] = pop_project_df["COUNTY_FIPS"].astype(str).str.zfill(5)
    
    # Merge population datasets
    pop_combined = pop_project_df.merge(
        pop_2023,
        on=['COUNTY_FIPS'],
        how='left'
    )
    
    # Rename and select columns
    pop_combined.rename(columns={"B01003_001E": "POPULATION_2023"}, inplace=True)
    pop_combined = pop_combined[[
        "COUNTY_FIPS", "STATE", "COUNTY", "NAME", "POPULATION_2023", 
        "POPULATION_2065_S3", "POPULATION_2065_S5b", "POPULATION_2065_S5a", 
        "POPULATION_2065_S5c", "CLIMATE_REGION", "POPULATION_2010"
    ]]
    
    # Calculate percentage changes for each scenario
    pop_combined["S3_Percentage_Change"] = ((pop_combined["POPULATION_2065_S3"] - pop_combined["POPULATION_2023"]) / pop_combined["POPULATION_2023"]) * 100
    pop_combined["S5b_Percentage_Change"] = ((pop_combined["POPULATION_2065_S5b"] - pop_combined["POPULATION_2023"]) / pop_combined["POPULATION_2023"]) * 100
    pop_combined["S5a_Percentage_Change"] = ((pop_combined["POPULATION_2065_S5a"] - pop_combined["POPULATION_2023"]) / pop_combined["POPULATION_2023"]) * 100
    pop_combined["S5c_Percentage_Change"] = ((pop_combined["POPULATION_2065_S5c"] - pop_combined["POPULATION_2023"]) / pop_combined["POPULATION_2023"]) * 100
    
    return pop_combined

def calculate_projected_values(df, base_year, percentage_change, scenario_name):
    """Calculate projected values based on percentage change"""
    projected_df = df[df["YEAR"] == base_year].copy()
    projected_df["SCENARIO"] = scenario_name
    
    # Exclude columns that should not be scaled
    columns_to_exclude = ["COUNTY_FIPS", "STATE", "COUNTY", "YEAR", "NAME", "SCENARIO"]
    numeric_cols = [col for col in df.columns if col not in columns_to_exclude]
    
    # Scale the numeric columns for the projected values
    for col in numeric_cols:
        projected_df[col] = round(projected_df[col] * (1 + percentage_change / 100))
    
    return projected_df

def generate_county_projections(filtered_df, pop_combined):
    """Generate projections for all counties under different scenarios"""
    # Get all unique counties
    all_counties = filtered_df['COUNTY_FIPS'].unique()
    
    # Create an empty DataFrame to store all results
    all_counties_2065_combined = pd.DataFrame()
    
    # Process each county
    for county in all_counties:
        # Filter data for the current county
        county_df = filtered_df[filtered_df['COUNTY_FIPS'] == county].copy()
        
        # Get original data for base year
        original_df = county_df[county_df["YEAR"] == 2023].copy()
        original_df["SCENARIO"] = "Original"
        
        # Extract this county's percentage changes
        county_proj = pop_combined[pop_combined["COUNTY_FIPS"] == county]
        if county_proj.empty:
            print(f"Skipping COUNTY_FIPS {county} - no projection data found.")
            continue
            
        percentage_changes = county_proj[
            ["S3_Percentage_Change", "S5b_Percentage_Change", "S5a_Percentage_Change", "S5c_Percentage_Change"]
        ].iloc[0].to_dict()
        
        # Calculate projections for each scenario
        s3_2065 = calculate_projected_values(county_df, base_year=2023,
                                           percentage_change=percentage_changes["S3_Percentage_Change"],
                                           scenario_name="S3")
        
        s5b_2065 = calculate_projected_values(county_df, base_year=2023,
                                            percentage_change=percentage_changes["S5b_Percentage_Change"],
                                            scenario_name="S5b")
        
        s5a_2065 = calculate_projected_values(county_df, base_year=2023,
                                            percentage_change=percentage_changes["S5a_Percentage_Change"],
                                            scenario_name="S5a")
        
        s5c_2065 = calculate_projected_values(county_df, base_year=2023,
                                            percentage_change=percentage_changes["S5c_Percentage_Change"],
                                            scenario_name="S5c")
        
        # Combine all scenarios for this county
        county_2065_combined = pd.concat([original_df, s3_2065, s5b_2065, s5a_2065, s5c_2065],
                                       ignore_index=True)
        
        # Add to the master DataFrame
        all_counties_2065_combined = pd.concat([all_counties_2065_combined, county_2065_combined],
                                             ignore_index=True)
    
    return all_counties_2065_combined

def calculate_derived_metrics(all_counties_2065_combined, merged_df):
    """Calculate derived metrics for projected data"""
    merged_df_2023 = merged_df[merged_df["YEAR"] == 2023].copy()
    merged_df_2023["COUNTY_FIPS"] = merged_df_2023["COUNTY_FIPS"].astype(str).str.zfill(5)
    all_counties = merged_df_2023['COUNTY_FIPS'].unique()
    
    for county in all_counties:
        all_counties_2065_combined["COUNTY_FIPS"] = all_counties_2065_combined["COUNTY_FIPS"].astype(str).str.zfill(5)
        county_df = merged_df_2023[merged_df_2023['COUNTY_FIPS'] == county].copy()
        teachers_2023 = county_df["PUBLIC_SCHOOL_TEACHERS"]
        housing_units_2023 = county_df["TOTAL_HOUSING_UNITS"]
        employed_population_2023 = county_df["TOTAL_EMPLOYED_POPULATION"]
        
        if teachers_2023.empty or housing_units_2023.empty or employed_population_2023.empty:
            print(f"Missing data for COUNTY_FIPS {county}")
            
        # Ensure the correct teacher count is applied for each county
        teacher_count = teachers_2023.values[0] if not teachers_2023.empty else 1  # Avoid division by zero
        all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "STUDENT_TEACHER_RATIO"] = (
            all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "PUBLIC_SCHOOL_STUDENTS"] / teacher_count
        )
        
        housing_units_count = housing_units_2023.values[0] if not housing_units_2023.empty else 1  # Avoid division by zero
        all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "AVAILABLE_HOUSING_UNITS"] = (
            housing_units_count - all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "OCCUPIED_HOUSING_UNITS"])
        
        employed_population_count = employed_population_2023.values[0] if not employed_population_2023.empty else 1  # Avoid division by zero
        all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "TOTAL_EMPLOYED_PERCENTAGE"] = (
            employed_population_count / all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "TOTAL_LABOR_FORCE"]) * 100
        
        all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "UNEMPLOYMENT_RATE"] = (
            100 - all_counties_2065_combined.loc[all_counties_2065_combined['COUNTY_FIPS'] == county, "TOTAL_EMPLOYED_PERCENTAGE"])
    
    # Format the state and county codes
    all_counties_2065_combined["STATE"] = all_counties_2065_combined["STATE"].astype(str).str.zfill(2)
    all_counties_2065_combined["COUNTY"] = all_counties_2065_combined["COUNTY"].astype(str).str.zfill(3)
    
    return all_counties_2065_combined

def calculate_indices(all_counties_2065_combined):
    """Calculate socioeconomic indices from the projected data"""
    # Filter to include only counties with school data
    index_df = all_counties_2065_combined[all_counties_2065_combined['PUBLIC_SCHOOL_STUDENTS'] > 0].copy()
    
    # Columns to standardize
    cols = ['UNEMPLOYMENT_RATE', 'STUDENT_TEACHER_RATIO', 'AVAILABLE_HOUSING_UNITS']
    
    # Standardize the data
    df_scaled = index_df.copy()
    z_scaler = StandardScaler()
    df_scaled[[f'z_{c}' for c in cols]] = z_scaler.fit_transform(index_df[cols])
    
    # Calculate different indices with different weights
    # Flip unemployment and student-teacher ratio (lower is better)
    df_scaled["INDEX_BALANCED"] = (
        (-df_scaled["z_UNEMPLOYMENT_RATE"]) * 0.33 + 
        (-df_scaled["z_STUDENT_TEACHER_RATIO"]) * 0.33 + 
        df_scaled["z_AVAILABLE_HOUSING_UNITS"] * 0.33
    )
    
    df_scaled["INDEX_EMPLOYMENT"] = (
        (-df_scaled["z_UNEMPLOYMENT_RATE"]) * 0.6 + 
        (-df_scaled["z_STUDENT_TEACHER_RATIO"]) * 0.2 + 
        df_scaled["z_AVAILABLE_HOUSING_UNITS"] * 0.2
    )
    
    df_scaled["INDEX_HOUSING"] = (  # Fixed typo in variable name
        (-df_scaled["z_UNEMPLOYMENT_RATE"]) * 0.2 + 
        (-df_scaled["z_STUDENT_TEACHER_RATIO"]) * 0.2 + 
        df_scaled["z_AVAILABLE_HOUSING_UNITS"] * 0.6
    )
    
    df_scaled["INDEX_EDUCATION"] = (
        (-df_scaled["z_UNEMPLOYMENT_RATE"]) * 0.2 + 
        (-df_scaled["z_STUDENT_TEACHER_RATIO"]) * 0.6 + 
        df_scaled["z_AVAILABLE_HOUSING_UNITS"] * 0.2
    )
    
    # Extract results and return
    results_df = df_scaled[['COUNTY_FIPS', 'SCENARIO', 'INDEX_BALANCED', 'INDEX_EMPLOYMENT', 'INDEX_HOUSING', 'INDEX_EDUCATION']]
    return results_df

def main():
    # Load and prepare data
    merged_df = load_and_merge_data()
    filtered_df = prepare_filtered_data(merged_df)
    
    # Process population data
    pop_combined = process_population_data()
    
    # Generate projections
    all_counties_2065_combined = generate_county_projections(filtered_df, pop_combined)
    
    # Calculate derived metrics
    all_counties_2065_combined = calculate_derived_metrics(all_counties_2065_combined, merged_df)
    
    # Save combined projected data
    all_counties_2065_combined.to_csv(PROJECTED_DATA / "combined_2065_data.csv", index=False)
    
    # Calculate socioeconomic indices
    results_df = calculate_indices(all_counties_2065_combined)
    
    # Save results
    output_path = PROJECTED_DATA / "projected_socioeconomic_indices.csv"
    results_df.to_csv(output_path, index=False)
    
    print(f"Analysis complete. Results saved to {output_path}")

if __name__ == "__main__":
    main()