import time
import os
from datetime import datetime


def log_event(log_category: str, message: str, path: str) -> None:
    """
    Log the event to a log file
    :param log_category: str - Category of the log event
    :param message: str - Message to log
    :param path: str - Path to log file
    :return: none
    """
    now: datetime = datetime.now()
    event: str = f"{now}, {log_category}, {message}\n"

    with open(path, "a") as file:
        file.write(event)


def read_file(path: str) -> str | None:
    """
    Read & return the content of the file
    :param path: str - Path to file
    :return: content or None
    """
    pass


def write_file(path: str) -> None:
    """
    Append events to the file
    :param path: str - Path to file
    :return: none
    """
    pass