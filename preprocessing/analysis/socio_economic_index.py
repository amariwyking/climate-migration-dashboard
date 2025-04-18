import pandas as pd
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler

# BASE_DIR = Path(".")
DATA_DIR = Path("./data")
PROCESSED_DIR = DATA_DIR / "processed"
CLEANED_DIR = PROCESSED_DIR / "cleaned_data"

# Input data paths
CRIME_DATA = CLEANED_DIR / "cleaned_crime_data.csv"
ECONOMIC_DATA = CLEANED_DIR / "cleaned_economic_data.csv"
EDUCATION_DATA = CLEANED_DIR / "cleaned_education_data.csv"
HOUSING_DATA = CLEANED_DIR / "cleaned_housing_data.csv"
JOB_OPENINGS_DATA = CLEANED_DIR / "cleaned_job_openings_data.csv"

def normalize_data(df, columns_to_normalize, invert_columns=None):
    """
    Normalize selected columns using Min-Max scaling.
    For some metrics (like crime rates), higher values are worse, so we invert them.
    """
    if invert_columns is None:
        invert_columns = []
    
    scaler = MinMaxScaler()
    df_normalized = df.copy()
    
    # Normalize specified columns
    if columns_to_normalize:
        normalized_values = scaler.fit_transform(df[columns_to_normalize])
        normalized_df = pd.DataFrame(normalized_values, columns=columns_to_normalize)
        
        # Invert columns where higher values are negative indicators
        for col in invert_columns:
            if col in normalized_df.columns:
                normalized_df[col] = 1 - normalized_df[col]
        
        # Replace original columns with normalized values
        for col in columns_to_normalize:
            df_normalized[col] = normalized_df[col]
    
    return df_normalized

def calculate_index(df, scenario):
    """
    Calculate socioeconomic index based on a specific weighting scenario.
    """
    # Create a copy of the dataframe to avoid modifying the original
    result_df = df.copy()
    
    # Initialize the index column
    index_col_name = f"socioeconomic_index_{scenario['name']}"
    result_df[index_col_name] = 0
    
    # Calculate weighted sum for each category
    for category, weight in scenario['weights'].items():
        category_cols = scenario['category_columns'][category]
        # Get only the columns that exist in the dataframe
        valid_cols = [col for col in category_cols if col in df.columns]
        
        if not valid_cols:
            print(f"Warning: No valid columns found for category {category}")
            continue
            
        # Calculate the average score for this category
        result_df[f"{category}_score"] = df[valid_cols].mean(axis=1)
        
        # Add weighted category score to the index
        result_df[index_col_name] += result_df[f"{category}_score"] * weight
    
    return result_df

def main():
    # Read all datasets
    crime_df = pd.read_csv(CRIME_DATA)
    economic_df = pd.read_csv(ECONOMIC_DATA)
    education_df = pd.read_csv(EDUCATION_DATA)
    housing_df = pd.read_csv(HOUSING_DATA)
    job_openings_df = pd.read_csv(JOB_OPENINGS_DATA)
    
    # Merge all dataframes on COUNTY_FIPS
    merged_df = crime_df.merge(
        economic_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'Year'], how='inner'
    ).merge(
        education_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'Year'], how='inner'
    ).merge(
        housing_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'Year'], how='inner'
    ).merge(
        job_openings_df, on=['COUNTY_FIPS','STATE','COUNTY','NAME','POPULATION', 'Year'], how='inner'
    )
    
    # Drop columns containing 'z_score'
    merged_df = merged_df.loc[:, ~merged_df.columns.str.contains('z_score', case=False)]
    
    # Define columns to normalize for each category
    normalization_config = {
        'crime': ['CRIMINAL_ACTIVITIES'],
        'economic': ['MEDIAN_INCOME', 'UNEMPLOYMENT_RATE', 'TOTAL_EMPLOYED_POPULATION'],
        'education': ['BACHELORS_OR_HIGHER_TOTAL', 'TOTAL_ENROLLED', 'LESS_THAN_HIGH_SCHOOL_UNEMPLOYED'],
        'housing': ['MEDIAN_HOUSING_VALUE', 'MEDIAN_GROSS_RENT', 'HOUSE_AFFORDABILITY'],
        'jobs': ['JOB_OPENING_JAN', 'JOB_OPENING_FEB', 'JOB_OPENING_MAR', 'JOB_OPENING_APR',
                'JOB_OPENING_MAY', 'JOB_OPENING_JUN', 'JOB_OPENING_JUL', 'JOB_OPENING_AUG',
                'JOB_OPENING_SEP', 'JOB_OPENING_OCT', 'JOB_OPENING_NOV', 'JOB_OPENING_DEC']
    }
    
    # Flatten the list of columns to normalize
    columns_to_normalize = [col for category_cols in normalization_config.values() for col in category_cols]
    
    # Define columns to invert (where higher values are negative indicators)
    invert_columns = ['Criminal_Activities', 'UNEMPLOYMENT_RATE', 'LESS_THAN_HIGH_SCHOOL_UNEMPLOYED', 
                      'HOUSE_AFFORDABILITY']
    
    # Normalize the data
    normalized_df = normalize_data(merged_df, columns_to_normalize, invert_columns)
    
    # Define different weighting scenarios
    scenarios = [
        {
            'name': 'balanced',
            'weights': {
                'crime': 0.2,
                'economic': 0.2,
                'education': 0.2,
                'housing': 0.2,
                'jobs': 0.2
            },
            'category_columns': {
                'crime': normalization_config['crime'],
                'economic': normalization_config['economic'],
                'education': normalization_config['education'],
                'housing': normalization_config['housing'],
                'jobs': normalization_config['jobs']
            }
        },
        {
            'name': 'economy_focused',
            'weights': {
                'crime': 0.1,
                'economic': 0.4,
                'education': 0.2,
                'housing': 0.2,
                'jobs': 0.1
            },
            'category_columns': {
                'crime': normalization_config['crime'],
                'economic': normalization_config['economic'],
                'education': normalization_config['education'],
                'housing': normalization_config['housing'],
                'jobs': normalization_config['jobs']
            }
        },
        {
            'name': 'safety_focused',
            'weights': {
                'crime': 0.4,
                'economic': 0.2,
                'education': 0.1,
                'housing': 0.2,
                'jobs': 0.1
            },
            'category_columns': {
                'crime': normalization_config['crime'],
                'economic': normalization_config['economic'],
                'education': normalization_config['education'],
                'housing': normalization_config['housing'],
                'jobs': normalization_config['jobs']
            }
        },
        {
            'name': 'opportunity_focused',
            'weights': {
                'crime': 0.1,
                'economic': 0.2,
                'education': 0.3,
                'housing': 0.1,
                'jobs': 0.3
            },
            'category_columns': {
                'crime': normalization_config['crime'],
                'economic': normalization_config['economic'],
                'education': normalization_config['education'],
                'housing': normalization_config['housing'],
                'jobs': normalization_config['jobs']
            }
        }
    ]
    
    # Calculate indices for all scenarios
    results_df = normalized_df.copy()
    for scenario in scenarios:
        results_df = calculate_index(results_df, scenario)
    
    # Keep only necessary columns for the final output
    # Identity columns + category scores + index scores
    identity_cols = ['COUNTY_FIPS', 'STATE', 'COUNTY', 'NAME', 'POPULATION', 'Year']
    category_score_cols = [col for col in results_df.columns if col.endswith('_score')]
    index_cols = [col for col in results_df.columns if col.startswith('socioeconomic_index_')]
    
    final_cols = identity_cols + category_score_cols + index_cols
    final_df = results_df[final_cols]
    
    # Save the final dataset with indices
    output_path = CLEANED_DIR / "socioeconomic_indices.csv"
    final_df.to_csv(output_path, index=False)
    
    print(f"Socioeconomic indices saved to: {output_path}")
    
    # Generate rankings for each index
    rankings_df = final_df.copy()
    for index_col in index_cols:
        rankings_df[f"{index_col}_rank"] = rankings_df[index_col].rank(ascending=False)
    
    rankings_path = CLEANED_DIR / "socioeconomic_indices_rankings.csv"
    rankings_df.to_csv(rankings_path, index=False)
    print(f"Rankings saved to: {rankings_path}")
    

if __name__ == "__main__":
    main()