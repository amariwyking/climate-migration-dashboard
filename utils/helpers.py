import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment-specific .env file
ENVIRONMENT = os.getenv(
    "ENVIRONMENT", "prod"
)  # Default to dev, change to prod when deploying
env_file = f".env.{ENVIRONMENT}" if ENVIRONMENT != "dev" else ".env"
load_dotenv(env_file)

# Fix Heroku connection string
DATABASE_URL = os.getenv("DATABASE_URL")

# Set SSL mode based on environment
SSL_MODE = "require" if ENVIRONMENT == "prod" else "disable"


def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        engine = create_engine(
            DATABASE_URL.replace("postgres://", "postgresql://", 1),
            connect_args={"sslmode": SSL_MODE},
        )
        conn = engine.connect()
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")
