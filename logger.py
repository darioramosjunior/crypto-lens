import time
import os
from datetime import datetime


def log_event(log_category, message, path):
    """
    Log the event to a log file
    :param log_category: str
    :param message: str
    :param path: str
    :return: none
    """
    now = datetime.now()
    event = f"{now}, {log_category}, {message}\n"

    with open(path, "a") as file:
        file.write(event)


def read_file(path):
    """
    Read & return the content of the file
    :param path: str
    :return: content
    """
    pass


def write_file(path):
    """
    Append events to the file
    :param path:
    :return: none
    """
    pass