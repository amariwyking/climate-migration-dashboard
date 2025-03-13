import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


def upload_csvs_to_postgres(folder_path: str, schema: str = "public") -> None:
    """
    Uploads all CSV files in a folder to PostgreSQL.
    - Uses the filename (without extension) as the table name
    - Replaces existing tables with fresh data
    """
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            # Extract table name from filename
            table_name = os.path.splitext(filename)[
                0
            ].lower()  # Lowercase for consistency
            filepath = os.path.join(folder_path, filename)

            # Read CSV
            df = pd.read_csv(filepath)

            try:
                # Upload to PostgreSQL
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists="replace",  # Overwrite existing data
                    index=False,
                    method="multi",  # Batch insert for speed
                    chunksize=1000,
                )
                print(f"Uploaded {filename} ➔ {schema}.{table_name}")
            except Exception as e:
                print(f"Error uploading {filename}: {e}")
                continue


if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    data_folder = script_dir.parent / "data" / "processed"

    print("\nUploading cleaned data...")
    upload_csvs_to_postgres(data_folder / "cleaned_data")

    print("\nUploading projected data...")
    upload_csvs_to_postgres(data_folder / "projected_data")

    print("\nAll data uploaded to PostgreSQL!")
