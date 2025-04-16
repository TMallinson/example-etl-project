import requests
import pandas as pd

def fetch_random_users(num_results: int = 500) -> pd.DataFrame:
    """
    Fetches random user data from the RandomUser API.
    
    Parameters:
        num_results: Number of user records to fetch.
        
    Returns:
        A pandas DataFrame containing user records.
    """
    url = f"https://randomuser.me/api/?results={num_results}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Normalize the JSON structure to a flat table
    users = pd.json_normalize(data['results'])
    return users