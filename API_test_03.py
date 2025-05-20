import requests
import os
import time
import logging
from dotenv import load_dotenv



# Load environment variables from .env
load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL")
API_KEY = os.getenv("API_KEY")
TANDEM_WEBHOOK_URL = os.getenv("TANDEM_WEBHOOK_URL")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", 300))  # Default: 5 minutes

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("tandem_sync.log"),
        logging.StreamHandler()
    ]
)

def fetch_sensor_data():
    try:
        headers = {"Authorization": f"Bearer {API_KEY}"}
        response = requests.get(f"{API_BASE_URL}/api/sensor-data", headers=headers)

        if response.status_code == 200:
            data = response.json()
            logging.info("Successfully fetched sensor data.")
            return data
        else:
            logging.error(f"Failed to fetch sensor data. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.exception("Error fetching sensor data.")
        return None

def transform_data_to_tandem_format(sensor_data):
    try:
        tandem_payload = []

        for device in sensor_data:
            tandem_payload.append({
                "deviceId": device.get("id"),
                "timestamp": device.get("timestamp"),
                "measurements": {
                    "Temperature": device.get("temperature"),
                    "Humidity": device.get("humidity"),
                    "CO2": device.get("co2"),
                    "Occupancy": 1 if device.get("co2", 0) > 500 else 0  # Example logic
                }
            })

        logging.info(f"Transformed {len(tandem_payload)} records to Tandem format.")
        return tandem_payload

    except Exception as e:
        logging.exception("Error transforming data.")
        return []

def send_to_tandem(data):
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(TANDEM_WEBHOOK_URL, json=data, headers=headers)

        if response.status_code in [200, 201, 202]:
            logging.info("Successfully sent data to Tandem.")
        else:
            logging.error(f"Failed to send data to Tandem. Status code: {response.status_code}, Response: {response.text}")

    except Exception as e:
        logging.exception("Error sending data to Tandem.")

def main():
    while True:
        logging.info("Starting new data sync cycle.")
        sensor_data = fetch_sensor_data()

        if sensor_data:
            tandem_data = transform_data_to_tandem_format(sensor_data)
            send_to_tandem(tandem_data)

        logging.info(f"Waiting {SYNC_INTERVAL} seconds before next sync...")
        time.sleep(SYNC_INTERVAL)

if __name__ == "__main__":
    main()
