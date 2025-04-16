import pandas as pd
import duckdb
from src.fetch_data import fetch_random_users
from src.enrich_data import enrich_users

# This code can be used to manually test the functions used through the ETL pipeline and visualize outputs

def test_fetch():
    print("Testing: Fetch Random Users")
    df = fetch_random_users()
    print(df.head())
    print(f"Total records fetched: {len(df)}")

def test_enrich():
    print("Testing: Enrich Users")
    df = fetch_random_users()
    enriched_df = enrich_users(df)
    print(enriched_df.head())

def test_duckdb_load():
    print("Testing: Data Load in DuckDB")
    # Assume the ETL pipeline was already run and stored data in "enriched_data.duckdb"
    con = duckdb.connect("enriched_data.duckdb")
    users = con.execute("SELECT * FROM users LIMIT 5").fetchdf()
    locations = con.execute("SELECT * FROM locations LIMIT 5").fetchdf()
    companies = con.execute("SELECT * FROM companies LIMIT 5").fetchdf()
    weather = con.execute("SELECT * FROM weather LIMIT 5").fetchdf()
    print("Users Table Sample:")
    print(users)
    print("\nLocations Table Sample:")
    print(locations)
    print("\nCompanies Table Sample:")
    print(companies)
    print("\nWeather Table Sample:")
    print(weather)
    con.close()

if __name__ == "__main__":
    test_fetch()
    print("\n" + "="*50 + "\n")
    test_enrich()
    print("\n" + "="*50 + "\n")
    test_duckdb_load()