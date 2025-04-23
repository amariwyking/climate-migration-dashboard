# Import required libraries
from pathlib import Path
import censusdis.data as ced
from censusdis.datasets import ACS5
import pandas as pd


def main():
    # Constants
    output_data_dir = Path("./data/processed/projected_data")
    output_data_dir.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE = output_data_dir / f"county_population_projections.csv"

    # Census 2065 population projection for the US
    CENSUS_POP_2065 = 366207000

    state_names = pd.read_csv(
        "./data/raw/state_data/state_names.csv",
        dtype={
            'STATE': str,
        }
    )

    us_county_data = pd.read_csv(
        "./data/raw/population_data/census_population_data_2010.csv",
        dtype={
            'COUNTY': str,
            'STATE': str,
        }
    )

    # Rename columns
    us_county_data = us_county_data.rename(
        columns={
            "STATE": "STATE_FIPS",
            "COUNTY": "COUNTY_FIPS",
            "NAME": "COUNTY_NAME",
            "B01003_001E": "POPULATION_2010",
        },
    )

    # Create full FIPS code
    us_county_data["COUNTY_FIPS"] = (
        us_county_data["STATE_FIPS"] + us_county_data["COUNTY_FIPS"]
    )

    # Add state names to the us county data
    us_county_data = us_county_data.merge(
        state_names, left_on="STATE_FIPS", right_on="STATE", how="left"
    )

    us_county_data = us_county_data.drop(columns=["STATE"])
    us_county_data = us_county_data.rename(columns={"NAME": "STATE_NAME"})

    # Define climate regions
    climate_regions = {
        "Northeast": [
            "Pennsylvania",
            "New Jersey",
            "New York",
            "Connecticut",
            "Rhode Island",
            "Massachusetts",
            "New Hampshire",
            "Vermont",
            "Maine",
        ],
        "South": [
            "District of Columbia",
            "Maryland",
            "Delaware",
            "Virginia",
            "West Virginia",
            "Kentucky",
            "North Carolina",
            "South Carolina",
            "Tennessee",
            "Alabama",
            "Georgia",
            "Florida",
            "Arkansas",
            "Mississippi",
            "Louisiana",
            "Oklahoma",
            "Texas",
        ],
        "Midwest": [
            "Montana",
            "Wyoming",
            "North Dakota",
            "South Dakota",
            "Nebraska",
            "Kansas",
            "Minnesota",
            "Iowa",
            "Missouri",
            "Wisconsin",
            "Illinois",
            "Michigan",
            "Indiana",
            "Ohio",
        ],
        "West": [
            "Washington",
            "Oregon",
            "Idaho",
            "Nevada",
            "Utah",
            "Colorado",
            "Arizona",
            "New Mexico",
        ],
        "California": ["California"],
    }

    # Map states to climate regions
    state_climate_regions = {
        state.lower(): region
        for region, states in climate_regions.items()
        for state in states
    }
    us_county_data["CLIMATE_REGION"] = (
        us_county_data["STATE_NAME"].str.lower().map(state_climate_regions)
    )

    # Aggregate population by climate region
    climate_region_populations = (
        us_county_data[["POPULATION_2010", "CLIMATE_REGION"]]
        .groupby(by="CLIMATE_REGION")
        .aggregate("sum")
    )

    # Calculate regional shares of the US population
    climate_region_shares = pd.DataFrame(
        climate_region_populations["POPULATION_2010"]
        .divide(climate_region_populations["POPULATION_2010"].sum())
        .multiply(100)
        .round(2)
    )

    # Define the forecasted 2065 regional population shares as presented in Table 5 of the Qin Fan et al. paper
    qf_2065_regional_population_shares = pd.DataFrame(
        {
            "Census_2010": [18.70, 20.77, 39.13, 8.84, 12.56],
            "Scenario_1": [12.48, 14.10, 46.23, 13.72, 13.47],
            "Scenario_3": [15.05, 21.33, 41.53, 8.78, 13.31],
            "Scenario_5": [16.42, 20.35, 38.18, 10.07, 14.98],
        },
        index=["Northeast", "Midwest", "South", "West", "California"],
    )

    climate_region_shares = climate_region_shares.merge(
        qf_2065_regional_population_shares, left_index=True, right_index=True
    )

    # Calculate 2065 population projections for Scenario 1
    climate_region_populations["POPULATION_2065_S1"] = (
        qf_2065_regional_population_shares["Scenario_1"]
        .divide(100)
        .multiply(CENSUS_POP_2065)
        .astype(int)
    )

    # Calculate 2065 population projections for Scenario 3
    climate_region_populations["POPULATION_2065_S3"] = (
        qf_2065_regional_population_shares["Scenario_3"]
        .divide(100)
        .multiply(CENSUS_POP_2065)
        .astype(int)
    )

    # Calculate climate migration effects for different scenarios
    climate_effect_on_pop_shares = pd.DataFrame(
        qf_2065_regional_population_shares["Scenario_5"]
        .div(qf_2065_regional_population_shares["Scenario_3"])
        .sub(1),
        columns=["scenario_5_100%"],
    )

    # Define climate migration intensities
    climate_effect_on_pop_shares["scenario_5_50%"] = climate_effect_on_pop_shares[
        "scenario_5_100%"
    ].multiply(0.50)
    climate_effect_on_pop_shares["scenario_5_200%"] = climate_effect_on_pop_shares[
        "scenario_5_100%"
    ].multiply(2.0)

    # Generate scenario variations
    def generate_scenario_5_variations(col):
        change_in_pop_share = qf_2065_regional_population_shares["Scenario_3"].mul(
            col)
        new_scenario_shares = qf_2065_regional_population_shares["Scenario_3"].add(
            change_in_pop_share
        )
        return new_scenario_shares

    alternate_scenario_5_pop_shares = climate_effect_on_pop_shares.apply(
        generate_scenario_5_variations, axis=0
    ).div(100)

    # Add scenario population projections to climate regions dataframe
    climate_region_populations = climate_region_populations.merge(
        alternate_scenario_5_pop_shares.mul(CENSUS_POP_2065).astype(int),
        left_index=True,
        right_index=True,
    ).rename(
        columns={
            "scenario_5_50%": "POPULATION_2065_S5a",
            "scenario_5_100%": "POPULATION_2065_S5b",
            "scenario_5_200%": "POPULATION_2065_S5c",
        }
    )

    # Calculate proportion of region for each county
    def calculate_proportion_of_region(row):
        county_population = row["POPULATION_2010"]
        climate_region = row["CLIMATE_REGION"]
        region_population = climate_region_populations.loc[climate_region][
            "POPULATION_2010"
        ]
        return county_population / region_population

    us_county_data["PERCENTAGE_OF_REGIONAL_POPULATION"] = us_county_data.apply(
        calculate_proportion_of_region, axis=1
    )

    # Calculate scenario populations for each county
    def calculate_county_scenario_populations(row):
        proportion_of_region = row["PERCENTAGE_OF_REGIONAL_POPULATION"]
        climate_region = row["CLIMATE_REGION"]
        migration_scenarios = climate_region_populations[
            [
                "POPULATION_2065_S1",
                "POPULATION_2065_S3",
                "POPULATION_2065_S5b",
                "POPULATION_2065_S5a",
                "POPULATION_2065_S5c",
            ]
        ]
        return (
            migration_scenarios.loc[climate_region]
            .mul(proportion_of_region)
            .astype(int)
        )

    us_county_population_projections = us_county_data.apply(
        calculate_county_scenario_populations, axis=1
    )

    # Merge projections with county data
    us_county_data = us_county_data.merge(
        us_county_population_projections, left_index=True, right_index=True
    )

    # us_county_data['COUNTY_FIPS'] = us_county_data['COUNTY_FIPS'].str.zfill(3)
    # us_county_data['STATE_FIPS'] = us_county_data['STATE_FIPS'].str.zfill(2)
    # us_county_data['COUNTY_FIPS'] = us_county_data['STATE_FIPS'] + \
    #     us_county_data['COUNTY_FIPS']

    us_county_data = us_county_data.set_index("COUNTY_FIPS")

    # Export the final dataset to CSV
    us_county_data.to_csv(OUTPUT_FILE)
    print(f"County population projections exported to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
