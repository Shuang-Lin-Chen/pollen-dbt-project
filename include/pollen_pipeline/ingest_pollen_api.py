import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import csv
import hashlib
import sys
from azure.storage.blob import BlobServiceClient

sys.stdout.reconfigure(encoding="utf-8")
# ============================================================
# 1. Load environment variables
# ============================================================
load_dotenv()

azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
azure_container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

# ============================================================
# 2. Open-Meteo Air Quality API endpoint
# ============================================================
url = "https://air-quality-api.open-meteo.com/v1/air-quality"

# ============================================================
# 3. Locations to ingest
# ============================================================
locations = [
    {
        "region": "Stockholm",
        "latitude": 59.3293,
        "longitude": 18.0686
    },
    {
        "region": "Göteborg",
        "latitude": 57.7089,
        "longitude": 11.9746
    },
    {
        "region": "Malmö",
        "latitude": 55.6050,
        "longitude": 13.0038
    },
    {
        "region": "Umeå",
        "latitude": 63.8258,
        "longitude": 20.2630
    }
]

# ============================================================
# 4. Pollen variables we want from Open-Meteo
# ============================================================
pollen_variables = [
    "alder_pollen",
    "birch_pollen",
    "grass_pollen",
    "mugwort_pollen",
    "olive_pollen",
    "ragweed_pollen"
]

# ============================================================
# 5. Fetch pollen forecast for each location
# ============================================================
all_location_responses = []
all_requests_successful = True
status_codes = []

for location in locations:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": ",".join(pollen_variables),
        "forecast_days": 4,
        "timezone": "Europe/Stockholm"
    }

    response = requests.get(url, params=params, timeout=30)

    print(f"{location['region']} Status Code:", response.status_code)

    status_codes.append(response.status_code)

    if response.status_code == 200:
        api_response = response.json()

        all_location_responses.append({
            "region": location["region"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "api_response": api_response
        })
    else:
        all_requests_successful = False
        print(f"API request failed for {location['region']}.")
        print(response.text)

# ============================================================
# 6. Continue only if all API requests succeeded
# ============================================================
if all_requests_successful:

    # ========================================================
    # 7. Read watermark file
    # ========================================================
    watermark_file = "watermark.json"

    if os.path.exists(watermark_file):
        with open(watermark_file, "r", encoding="utf-8") as file:
            watermark = json.load(file)
    else:
        watermark = {}

    last_payload_hash = watermark.get("last_payload_hash")

    # ========================================================
    # 8. Build stable payload for change detection
    #    Exclude unstable metadata such as generationtime_ms
    # ========================================================
    payload_for_hash = {
        "locations": [
            {
                "region": item["region"],
                "latitude": item["latitude"],
                "longitude": item["longitude"],
                "hourly": item["api_response"].get("hourly", {})
            }
            for item in all_location_responses
        ]
    }

    payload_string = json.dumps(
        payload_for_hash,
        sort_keys=True,
        ensure_ascii=False
    )

    current_payload_hash = hashlib.sha256(
        payload_string.encode("utf-8")
    ).hexdigest()

    print("Last payload hash from watermark:", last_payload_hash)
    print("Current payload hash:", current_payload_hash)

    # ========================================================
    # 9. Compare current payload hash with previous watermark
    # ========================================================
    has_new_data = current_payload_hash != last_payload_hash

    print("Has new data:", has_new_data)

    if has_new_data:

        # ====================================================
        # 10. Count regions and pollen forecast records
        # ====================================================
        region_count = len(all_location_responses)

        pollen_record_count = 0

        for item in all_location_responses:
            hourly_data = item["api_response"].get("hourly", {})
            hourly_times = hourly_data.get("time", [])

            # One hourly row per region per forecast time
            pollen_record_count += len(hourly_times)

        # ====================================================
        # 11. Wrap API response with ingestion metadata
        # ====================================================
        data = {
            "metadata": {
                "source_system": "Open-Meteo Air Quality API",
                "endpoint": url,
                "ingested_at": datetime.now().isoformat(),
                "locations": [location["region"] for location in locations],
                "forecast_days": 4,
                "pollen_variables": pollen_variables
            },
            "payload": {
                "locations": all_location_responses
            }
        }

        # ====================================================
        # 12. Create partitioned raw data folder
        # ====================================================
        ingestion_date = datetime.now().strftime("%Y-%m-%d")
        raw_folder = f"raw_data/ingestion_date={ingestion_date}"
        os.makedirs(raw_folder, exist_ok=True)

        # ====================================================
        # 13. Create file name
        # ====================================================
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{raw_folder}/pollen_forecast_{timestamp}.json"

        # ====================================================
        # 14. Save raw JSON locally
        # ====================================================
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

        print(f"Raw JSON file saved successfully: {file_path}")

        # ====================================================
        # 15. Upload raw JSON to Azure Blob Storage
        # ====================================================
        blob_service_client = BlobServiceClient.from_connection_string(
            azure_connection_string
        )

        container_client = blob_service_client.get_container_client(
            azure_container_name
        )

        blob_name = (
            f"ingestion_date={ingestion_date}/"
            f"pollen_forecast_{timestamp}.json"
        )

        with open(file_path, "rb") as file:
            container_client.upload_blob(
                name=blob_name,
                data=file,
                overwrite=True
            )

        print(
            "Raw JSON uploaded to Azure Blob Storage successfully:",
            blob_name
        )

        # ====================================================
        # 16. Update watermark after successful save + upload
        # ====================================================
        updated_watermark = {
            "last_payload_hash": current_payload_hash
        }

        with open(watermark_file, "w", encoding="utf-8") as file:
            json.dump(
                updated_watermark,
                file,
                indent=4,
                ensure_ascii=False
            )

        print("Watermark updated successfully.")

        # ====================================================
        # 17. Write successful ingestion log
        # ====================================================
        log_file = "ingestion_log.csv"
        file_exists = os.path.exists(log_file)

        with open(log_file, "a", newline="", encoding="utf-8") as log:
            writer = csv.writer(log)

            if not file_exists:
                writer.writerow([
                    "ingested_at",
                    "status_code",
                    "status",
                    "file_path",
                    "region_count",
                    "pollen_record_count"
                ])

            writer.writerow([
                data["metadata"]["ingested_at"],
                ",".join(str(code) for code in status_codes),
                "SUCCESS",
                file_path,
                region_count,
                pollen_record_count
            ])

        print(f"Ingestion log updated successfully: {log_file}")

    else:
        print("No new source data. Raw JSON save skipped.")

        # ====================================================
        # 18. Write skipped ingestion log
        # ====================================================
        log_file = "ingestion_log.csv"
        file_exists = os.path.exists(log_file)

        with open(log_file, "a", newline="", encoding="utf-8") as log:
            writer = csv.writer(log)

            if not file_exists:
                writer.writerow([
                    "ingested_at",
                    "status_code",
                    "status",
                    "file_path",
                    "region_count",
                    "pollen_record_count"
                ])

            writer.writerow([
                datetime.now().isoformat(),
                ",".join(str(code) for code in status_codes),
                "SKIPPED",
                "",
                0,
                0
            ])

        print(f"Skipped log written successfully: {log_file}")

else:
    # ========================================================
    # 19. Write failed ingestion log
    # ========================================================
    log_file = "ingestion_log.csv"
    file_exists = os.path.exists(log_file)

    with open(log_file, "a", newline="", encoding="utf-8") as log:
        writer = csv.writer(log)

        if not file_exists:
            writer.writerow([
                "ingested_at",
                "status_code",
                "status",
                "file_path",
                "region_count",
                "pollen_record_count"
            ])

        writer.writerow([
            datetime.now().isoformat(),
            ",".join(str(code) for code in status_codes),
            "FAILED",
            "",
            0,
            0
        ])

    print(f"Failure log written successfully: {log_file}")