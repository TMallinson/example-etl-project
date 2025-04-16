import pandas as pd
from fetch_data import fetch_random_users
from enrich_data import enrich_users
from store_data import create_and_load_schema

def run_batch_pipeline():
    print("Starting ETL Pipeline...")
    
    # Step 1: Fetch Data
    print("Fetching random users...")
    users_df = fetch_random_users()
    print(f"Fetched {len(users_df)} records.")
    
    # Export users_df to CSV for visualization
    print("Exporting user data to 'users_data.csv'...")
    users_df.to_csv("users_data.csv", index=False)
    
    # Step 2: Enrich Data
    print("Enriching user records with weather and company data...")
    enriched_df = enrich_users(users_df)
    print("Enriched Data Snapshot:")
    print(enriched_df.head())
    
    # Export enriched_df to CSV for visualization
    print("Exporting enriched data to 'enriched_data.csv'...")
    enriched_df.to_csv("enriched_data.csv", index=False)
    
    # Step 3: Store Data in DuckDB
    print("Storing enriched data in DuckDB...")
    create_and_load_schema(enriched_df)
    print("Data stored successfully in 'enriched_data.duckdb'.")

if __name__ == "__main__":
    run_batch_pipeline()