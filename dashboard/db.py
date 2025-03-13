import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file


def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        engine = create_engine(os.getenv("DATABASE_URL"))
        conn = engine.connect()
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")
