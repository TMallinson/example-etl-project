import duckdb
import pandas as pd

def create_and_load_schema(enriched_df: pd.DataFrame, db_path: str = "enriched_data.duckdb"):
    """
    Creates a normalized schema in DuckDB and loads the enriched data.

    The following tables are created:
      - users: User details and foreign keys for location and company.
      - locations: Location details (extracted from user info).
      - companies: Company information.
      - weather: Weather information linked by location.
      
    Parameters:
        enriched_df: DataFrame with enriched user records.
        db_path: Path to the DuckDB file.
    """
    con = duckdb.connect(database=db_path, read_only=False)
    
    # Drop tables if they already exist for idempotency (can rerun without negative effects downstream)
    tables = ["users", "locations", "companies", "weather"]
    for table in tables:
        con.execute(f"DROP TABLE IF EXISTS {table}")
    
    # Create separate DataFrames for each table:
    # Extract Locations
    locations_df = enriched_df[["latitude", "longitude"]].drop_duplicates().reset_index(drop=True)
    locations_df["location_id"] = locations_df.index + 1

    # Merge location_id back to enriched_df by matching latitude and longitude. This is to embed foreign key references in the main DataFrame
    enriched_df = enriched_df.merge(locations_df, on=["latitude", "longitude"], how="left")
    
    # Create Companies table
    companies_df = enriched_df[["company_name", "company_industry"]].drop_duplicates().reset_index(drop=True)
    companies_df["company_id"] = companies_df.index + 1

    # Merge company_id back to enriched_df
    enriched_df = enriched_df.merge(companies_df, on=["company_name", "company_industry"], how="left")
    
    # Create Weather table (weather details are stored per location)
    weather_df = locations_df.copy()
    weather_df["weather_temperature"] = enriched_df.groupby("location_id")["weather_temperature"].first().values
    weather_df["weather_windspeed"] = enriched_df.groupby("location_id")["weather_windspeed"].first().values
    
    # Create Users table (store only the relevant columns)
    users_cols = ["name.first", "name.last", "email", "phone", "location_id", "company_id"]
    # Some of these fields may not exist if the API did not return them; use get with default.
    enriched_df["name.first"] = enriched_df.get("name.first", enriched_df.get("name.first", None))
    enriched_df["name.last"] = enriched_df.get("name.last", None)
    enriched_df["email"] = enriched_df.get("email", None)
    enriched_df["phone"] = enriched_df.get("phone", None)
    users_df = enriched_df[users_cols].copy().reset_index(drop=True)
    users_df["user_id"] = users_df.index + 1

    # Create temp views that map to the DataFrames
    con.register("locations_temp", locations_df)
    con.register("companies_temp", companies_df)
    con.register("weather_temp", weather_df)
    con.register("users_temp", users_df)

    # Write DataFrames to DuckDB tables.
    con.execute("CREATE TABLE locations AS SELECT * FROM locations_temp")
    con.execute("CREATE TABLE companies AS SELECT * FROM companies_temp")
    con.execute("CREATE TABLE weather AS SELECT * FROM weather_temp")
    con.execute("CREATE TABLE users AS SELECT * FROM users_temp")
    con.commit()
    con.close()