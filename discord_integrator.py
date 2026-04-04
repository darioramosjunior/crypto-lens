import requests
import os
import logger
from datetime import datetime

script_path = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(script_path, "logs", "discord_integrator.txt")
image_path = os.path.join(script_path, "hourly_market_pulse", "market_pulse.png")
webhook_url = ""
now = datetime.now()


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