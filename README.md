# climate-migration-dashboard

## Windows Setup

1. Clone the repo.
2. Create a virtual environment: `python -m venv .venv`
3. Activate it: `.venv\Scripts\activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Install Docker desktop
6. Run `docker-compose up -d` to run container and `docker-compose down` to stop container when done with project.
7. Create an .env file and add your `US_CENSUS_API_KEY` and postgresSQL `DATABASE_URL` to it
8. Create directory and txt file to save postgress password `secrets\postgres_passwod.txt`
9. Run `.\pipeline.bat`
