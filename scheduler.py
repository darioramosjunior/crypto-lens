#!/usr/bin/env python3
"""Scheduler service that runs pipeline and cleanup jobs on schedule using APScheduler.

Jobs:
- run_scripts.sh at minute 56 of every hour
- clean_logs.sh at 00:00 daily

Logs are written to ./logs/scheduler.log
"""

import logging
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "scheduler.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("scheduler")

PY = os.environ.get("PYTHON_PATH", "python3")
RUN_SCRIPT = ROOT / "run_scripts.sh"
CLEAN_SCRIPT = ROOT / "clean_logs.sh"

# Helper to execute a shell script and log output

def run_script(script_path: Path):
    script = str(script_path)
    logger.info(f"Starting script: {script}")
    try:
        result = subprocess.run(["/bin/bash", script], check=False, capture_output=True, text=True)
        if result.stdout:
            logger.info(f"stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"stderr:\n{result.stderr}")
        if result.returncode != 0:
            logger.error(f"Script {script} exited with code {result.returncode}")
    except Exception as e:
        logger.exception(f"Failed to run script {script}: {e}")


def job_run_pipeline():
    run_script(RUN_SCRIPT)


def job_clean_logs():
    run_script(CLEAN_SCRIPT)


def listener(event):
    if event.exception:
        logger.error(f"Job raised an exception: {event.job_id}")
    else:
        logger.info(f"Job executed: {event.job_id}")


def main():
    logger.info("Scheduler starting up")
    sched = BlockingScheduler(timezone="UTC")

    # Run at minute 56 every hour
    sched.add_job(job_run_pipeline, CronTrigger(minute="56"), id="run_pipeline")

    # Run daily clean at midnight UTC (00:00)
    sched.add_job(job_clean_logs, CronTrigger(hour="0", minute="0"), id="clean_logs")

    sched.add_listener(listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler shutting down")


if __name__ == "__main__":
    main()
