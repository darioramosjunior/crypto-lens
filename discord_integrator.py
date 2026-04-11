import requests
import os
import logger
import config
from datetime import datetime

# Ensure log directory exists
config.ensure_log_directory()

script_path = os.path.dirname(os.path.abspath(__file__))
log_path = config.get_log_file_path("discord_integrator")
image_path = os.path.join(script_path, "hourly_market_pulse", "market_pulse.png")
webhook_url = ""
now = datetime.now()

# Create log file if it doesn't exist
try:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if not os.path.exists(log_path):
        open(log_path, 'a').close()
except Exception as e:
    print(f"[WARNING] Failed to create log file {log_path}: {e}")


def send_to_discord(webhook_url, message):
    response = requests.post(
        webhook_url,
        data={
            "content": message
        }
    )
    if response.status_code == 200:
        logger.log_event(log_category="INFO", message="Successfully sent message to Discord", path=log_path)
    else:
        logger.log_event(log_category="ERROR", message=f"Failed to send message. Status: {response.status_code}, Response: {response.text}", path=log_path)

def upload_to_discord(webhook_url, image_path, message=f"{now} - Latest Hourly Market hourly_market_pulse"):
    with open(image_path, 'rb') as f:
        response = requests.post(
            webhook_url,
            data={
                "content": message
            },
            files={
                "file": f
            }
        )
    if response.status_code == 200:
        logger.log_event(log_category="INFO", message="Successfully uploaded image to Discord", path=log_path)
    else:
        logger.log_event(log_category="ERROR", message=f"Failed to upload image. Status: {response.status_code}, Response: {response.text}", path=log_path)