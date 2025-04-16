import requests
import pandas as pd
from faker import Faker
import random

fake = Faker()

def fetch_weather(latitude: float, longitude: float) -> dict:
    """
    Retrieves current weather data from the Open-Meteo API for given coordinates.
    
    Parameters:
        latitude: Latitude value.
        longitude: Longitude value.
        
    Returns:
        Dictionary containing weather details.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": True
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    weather_data = response.json()

    # Extract current weather
    return weather_data.get("current_weather", {})

def generate_company_info() -> dict:
    """
    Generates random company information using Faker.
    
    Returns:
        A dictionary with company details.
    """
    return {
        "company_name": fake.company(),
        "industry": random.choice(["Technology", "Finance", "Health", "Retail", "Education"])
    }

def enrich_user_record(user: dict) -> dict:
    """
    Enriches an individual user record with weather and company data.
    
    Parameters:
        user: A dictionary representing a single user record.
        
    Returns:
        The enriched user record as a dictionary.
    """
    # Extract latitude and longitude from the nested JSON (as strings, convert to float)
    try:
        coords = user.get("location.coordinates", {})
        lat = float(coords.get("latitude"))
        lon = float(coords.get("longitude"))
    except (ValueError, TypeError):
        lat = lon = None

    weather = {}
    if lat is not None and lon is not None:
        try:
            weather = fetch_weather(lat, lon)
        except Exception as e:
            weather = {}

    company = generate_company_info()

    # Combine the records; add weather and company info to the user dictionary
    enriched_user = user.copy()
    enriched_user.update({
        "latitude": lat,
        "longitude": lon,
        "weather_temperature": weather.get("temperature"),
        "weather_windspeed": weather.get("windspeed"),
        "company_name": company["company_name"],
        "company_industry": company["industry"]
    })
    return enriched_user

def enrich_users(users_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches a DataFrame of users with weather and company data.
    
    Parameters:
        users_df: DataFrame with user records.
        
    Returns:
        DataFrame with enriched user records.
    """
    enriched_records = []
    for _, row in users_df.iterrows():
        # Convert row to dict instead of series
        user_dict = row.to_dict()
        enriched_user = enrich_user_record(user_dict)
        enriched_records.append(enriched_user)
    return pd.DataFrame(enriched_records)