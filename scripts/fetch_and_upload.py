import requests
import json
from datetime import datetime
import os
from pathlib import Path
from google.cloud import storage

# Support for .env file
try:
    from dotenv import load_dotenv
    project_root = Path(__file__).parent.parent
    load_dotenv(project_root / '.env')
except ImportError:
    print("python-dotenv not installed. Using system environment variables only.")

# OpenWeather API Configuration
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY environment variable is not set")

CITY = os.getenv("WEATHER_CITY", "Paris")
URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def fetch_weather():
    """Fetch current weather data from OpenWeather API"""
    try:
        print(f"  Fetching weather data for {CITY}...")
        response = requests.get(URL, timeout=10)
        
        if response.status_code != 200:
            print(f" API Error: Status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
        
        data = response.json()
        
        if "main" not in data:
            print(f" Error: 'main' key not found in response")
            return None

        weather_data = {
            "city": CITY,
            "country": data["sys"]["country"],
            "timestamp": datetime.utcnow().isoformat(),
            "date": datetime.utcnow().strftime('%Y-%m-%d'),
            "time": datetime.utcnow().strftime('%H:%M:%S'),
            "temperature": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "temp_min": data["main"]["temp_min"],
            "temp_max": data["main"]["temp_max"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "weather": data["weather"][0]["description"],
            "weather_main": data["weather"][0]["main"],
            "wind_speed": data.get("wind", {}).get("speed", 0),
            "wind_deg": data.get("wind", {}).get("deg", 0),
            "clouds": data.get("clouds", {}).get("all", 0),
            "sunrise": datetime.fromtimestamp(data["sys"]["sunrise"]).isoformat(),
            "sunset": datetime.fromtimestamp(data["sys"]["sunset"]).isoformat(),
            "visibility": data.get("visibility", 0),
            "coord_lat": data["coord"]["lat"],
            "coord_lon": data["coord"]["lon"]
        }

        print(f"Data fetched successfully!")
        print(f"    Temperature: {weather_data['temperature']}°C (feels like {weather_data['feels_like']}°C)")
        print(f"    Humidity: {weather_data['humidity']}%")
        print(f"    Weather: {weather_data['weather']}")
        
        return weather_data

    except requests.exceptions.RequestException as e:
        print(f" Network error: {e}")
        return None
    except Exception as e:
        print(f" Unexpected error: {e}")
        return None

def save_locally(weather_data):
    """Save weather data to local JSON file"""
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    filename = data_dir / f"weather_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(weather_data, f, ensure_ascii=False)
    
    print(f" Saved locally: {filename}")
    return filename

def upload_to_gcs(local_file, weather_data):
    """Upload weather data to Google Cloud Storage"""
    
    if not GCP_PROJECT_ID or not GCS_BUCKET_NAME:
        print("  GCP not configured. Skipping upload to Cloud Storage.")
        print("   Set GCP_PROJECT_ID and GCS_BUCKET_NAME in .env to enable.")
        return False
    
    try:
        # Initialize GCS client
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        
        # Create blob path: raw/YYYY/MM/DD/weather_YYYYMMDD_HHMMSS.json
        date = datetime.utcnow()
        blob_path = f"raw/{date.year}/{date.month:02d}/{date.day:02d}/{local_file.name}"
        
        blob = bucket.blob(blob_path)
        
        # Upload the file
        blob.upload_from_filename(str(local_file))
        
        print(f"  Uploaded to GCS: gs://{GCS_BUCKET_NAME}/{blob_path}")
        return True
        
    except Exception as e:
        print(f" Error uploading to GCS: {e}")
        print("   Make sure you have:")
        print("   1. Created a GCS bucket")
        print("   2. Set up authentication (gcloud auth application-default login)")
        print("   3. Updated .env with GCP_PROJECT_ID and GCS_BUCKET_NAME")
        return False

def main():
    """Main pipeline function"""
    print("=" * 60)
    print(" Weather Data Pipeline - Starting...")
    print("=" * 60)
    
    # Step 1: Fetch weather data
    weather_data = fetch_weather()
    if not weather_data:
        print(" Pipeline failed: Could not fetch weather data")
        return
    
    # Step 2: Save locally
    local_file = save_locally(weather_data)
    
    # Step 3: Upload to GCS
    upload_to_gcs(local_file, weather_data)
    
    print("=" * 60)
    print(" Pipeline completed!")
    print("=" * 60)

if __name__ == "__main__":
    main()