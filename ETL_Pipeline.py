from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine



# Database connection string
DATABASE_URL = "mssql+pyodbc://DESKTOP-GTDNBNA\\SQLEXPRESS/etl_pipeline?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
API_KEY = "e72636399c1196fd11780bec8a428fb7"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


# Cities
european_cities = [
    "London", "Manchester", "Birmingham", "Edinburgh", "Glasgow",
    "Paris", "Marseille", "Lyon", "Toulouse", "Bordeaux", "Lille", "Nice",
    "Berlin", "Hamburg", "Munich", "Frankfurt", "Cologne", "Düsseldorf", "Stuttgart",
    "Madrid", "Barcelona", "Valencia", "Seville", "Bilbao", "Zaragoza", "Malaga",
    "Lisbon", "Porto", "Braga", "Coimbra",
    "Rome", "Milan", "Turin", "Florence", "Naples", "Venice", "Bologna",
    "Brussels", "Antwerp", "Ghent", "Bruges",
    "Amsterdam", "Rotterdam", "The Hague", "Utrecht",
    "Zurich", "Geneva", "Bern", "Basel", "Lausanne",
    "Vienna", "Salzburg", "Innsbruck", "Graz",
    "Dublin", "Cork", "Galway", "Limerick",
    "Luxembourg City",
    "Monaco",  "Casablanca", "Rabat", "Marrakech", "Fez", "Tangier", "Agadir",
    "Algiers", "Oran", "Constantine", "Annaba", "Blida",
    "Tunis", "Sfax", "Sousse", "Bizerte", "Kairouan",
    "Tripoli", "Benghazi", "Misrata", "Sabha",
    "Cairo", "Alexandria", "Giza", "Luxor", "Aswan"
]

# Function: Extract
def extract(city):
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.text}")
        return None

# Function: Transform
def transform(raw_data):
    # Check if raw_data is valid
    if not raw_data:
        return pd.DataFrame()  # Return an empty DataFrame if no data
    # Extract the required fields
    transformed_data = {
        "City": raw_data.get("name"),
        "Country": raw_data.get("sys", {}).get("country"),
        "Latitude": raw_data.get("coord", {}).get("lat"),
        "Longitude": raw_data.get("coord", {}).get("lon"),
        "Weather": raw_data.get("weather", [{}])[0].get("main"),
        "Cloudiness": raw_data.get("clouds", {}).get("all"),
        "Icon": raw_data.get("weather", [{}])[0].get("icon"),
        "Temperature": raw_data.get("main", {}).get("temp"),
        "Feels_Like": raw_data.get("main", {}).get("feels_like"),
        "Wind_Speed": raw_data.get("wind", {}).get("speed"),
        "Wind_Direction": raw_data.get("wind", {}).get("deg"),
        "Pressure": raw_data.get("main", {}).get("pressure"),
        "Humidity": raw_data.get("main", {}).get("humidity"),
        "Precipitation": raw_data.get("rain", {}).get("1h", 0),  # Default to 0 if no rain data
        "Sunrise": raw_data.get("sys", {}).get("sunrise"),
        "Sunset": raw_data.get("sys", {}).get("sunset"),
    }

    # Convert to DataFrame
    df = pd.DataFrame([transformed_data])

    # Convert timestamps to datetime
    df["Sunrise"] = pd.to_datetime(df["Sunrise"], unit="s", errors='coerce')
    df["Sunset"] = pd.to_datetime(df["Sunset"], unit="s", errors='coerce')

    return df

# Function: Load
def load(df):
    # Create the database engine
    engine = create_engine(DATABASE_URL)
    # Load the data into SQL Server
    df.to_sql("weather_data", engine, index=False, if_exists="append")


if __name__ == "__main__":
    for city in european_cities:
        raw_data = extract(city)
        df = transform(raw_data)
        load(df)
        print("Data loaded successfully!")