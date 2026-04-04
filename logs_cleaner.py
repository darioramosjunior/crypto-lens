import os
from glob import glob

script_path = os.path.dirname(os.path.abspath(__file__))
logs_path = os.path.join(script_path, "logs")


def delete_content():
    for path in glob(os.path.join(logs_path, "*.txt")):
        print(path)
        with open(path, 'w') as file:
            file.write("")


if __name__ == "__main__":
    print(f"Running {__file__}")
    print(f"Script path: {script_path}")
    print(f"Logs path: {logs_path}")
    try:
        delete_content()
        print("Successfully cleaned log files...")
    except Exception as e:
        print("Failed to clean log files...")