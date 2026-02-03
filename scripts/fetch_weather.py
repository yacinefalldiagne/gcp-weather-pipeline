import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY environment variable is not set")

CITY = "Paris"
URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

def fetch_weather():
    response = requests.get(URL)
    data = response.json()
    
    # Check if the API call was successful
    if response.status_code != 200:
        print(f"API Error: Status code {response.status_code}")
        print(f"Response: {data}")
        return
    
    if "main" not in data:
        print(f"Error: 'main' key not found in response")
        print(f"Response keys: {data.keys()}")
        print(f"Full response: {json.dumps(data, indent=2)}")
        return

    weather_data = {
        "city": CITY,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "weather": data["weather"][0]["description"]
    }

    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)
    
    filename = os.path.join(data_dir, f"weather_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json")

    with open(filename, "w") as f:
        json.dump(weather_data, f, indent=4)

    print("Weather data saved:", filename)

if __name__ == "__main__":
    fetch_weather()