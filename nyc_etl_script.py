import requests
import csv
import boto3
import os
from datetime import datetime

API_KEY = os.environ["OPENWEATHERMAP_API_KEY"]
CITY = "New York"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

response = requests.get(URL)
data = response.json()

processed_data = {
    "timestamp": datetime.now().isoformat(),
    "city": data["name"],
    "country": data["sys"]["country"],
    "weather": data["weather"][0]["description"],
    "temp": data["main"]["temp"],
}

filename = f"{CITY.replace(' ', '_').lower()}_{processed_data['timestamp'].replace(':', '-')}.csv"

s3 = boto3.client("s3")
s3_bucket = os.environ["S3_BUCKET"]

# Write processed_data to a CSV file
header = ["timestamp", "city", "country", "weather", "temp"]
with open(filename, "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()
    writer.writerow(processed_data)

s3.upload_file(filename, s3_bucket, f"weather_data/{filename}")

os.remove(filename)