import requests
import os
import logger
import config
from datetime import datetime
from typing import Optional

# Ensure log directory exists
config.ensure_log_directory()

script_path: str = os.path.dirname(os.path.abspath(__file__))
log_path: str = config.get_log_file_path("discord_integrator")
image_path: str = os.path.join(script_path, "hourly_market_pulse", "market_pulse.png")
webhook_url: str = ""
now: datetime = datetime.now()

# Create log file if it doesn't exist
try:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if not os.path.exists(log_path):
        open(log_path, 'a').close()
except Exception as e:
    print(f"[WARNING] Failed to create log file {log_path}: {e}")


def send_to_discord(webhook_url: str, message: str) -> None:
    """
    Send a message to Discord via webhook
    :param webhook_url: str - Discord webhook URL
    :param message: str - Message to send
    :return: None
    """
    response: requests.Response = requests.post(
        webhook_url,
        data={
            "content": message
        }
    )
    if response.status_code == 200:
        logger.log_event(log_category="INFO", message="Successfully sent message to Discord", path=log_path)
    else:
        logger.log_event(log_category="ERROR", message=f"Failed to send message. Status: {response.status_code}, Response: {response.text}", path=log_path)

def upload_to_discord(webhook_url: str, image_path: str, message: Optional[str] = None) -> None:
    """
    Upload an image to Discord via webhook
    :param webhook_url: str - Discord webhook URL
    :param image_path: str - Path to image file
    :param message: str - Optional message to include with image
    :return: None
    """
    if message is None:
        message = f"{now} - Latest Hourly Market hourly_market_pulse"
    
    with open(image_path, 'rb') as f:
        response: requests.Response = requests.post(
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