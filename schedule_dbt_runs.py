"""
Scheduled dbt Pipeline Runner for Snowflake Cost Optimisation Framework.

Runs the dbt pipeline at configured times (10:30 AM, 1:00 PM, 4:00 PM)
to keep cost models refreshed throughout the business day.

Usage:
    python schedule_dbt_runs.py              # Run scheduler (stays running)
    python schedule_dbt_runs.py --run-now    # Run pipeline once immediately
    python schedule_dbt_runs.py --full       # Run with seeds + tests
    python schedule_dbt_runs.py --install    # Install as Windows Task Scheduler jobs

ACCOUNT_USAGE latency is ~45 minutes, so:
    10:30 AM run captures data through ~9:45 AM
     1:00 PM run captures data through ~12:15 PM
     4:00 PM run captures data through ~3:15 PM
"""

import subprocess
import sys
import os
import time
import argparse
import logging
from datetime import datetime, timedelta

# --- Configuration ---
CONNECTION = "cost_optimization"
DBT_PROJECT = "cost_optimization"
SCHEDULE_TIMES = ["10:30", "13:00", "16:00"]  # 24-hour format
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
SNOW_CLI = "snow"  # Snowflake CLI command

# --- Logging Setup ---
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"dbt_scheduler_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def run_command(cmd, description):
    """Run a shell command and return success/failure."""
    logger.info(f"Starting: {description}")
    logger.info(f"Command: {cmd}")

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=600,  # 10-minute timeout per command
        )

        if result.stdout:
            logger.info(f"Output:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr:\n{result.stderr}")

        if result.returncode == 0:
            logger.info(f"Completed: {description}")
            return True
        else:
            logger.error(f"Failed (exit code {result.returncode}): {description}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout after 600s: {description}")
        return False
    except Exception as e:
        logger.error(f"Error running {description}: {e}")
        return False


def run_dbt_pipeline(include_seeds=False, include_tests=True):
    """Run the full dbt pipeline."""
    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info(f"dbt Pipeline Run Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    results = {}

    # Step 1: Seeds (only if requested or config changed)
    if include_seeds:
        results["seed"] = run_command(
            f"{SNOW_CLI} dbt execute {DBT_PROJECT} seed --connection {CONNECTION}",
            "dbt seed (load reference data)",
        )

    # Step 2: Run all models (staging views + intermediate tables + publication tables + alerts)
    results["run"] = run_command(
        f"{SNOW_CLI} dbt execute {DBT_PROJECT} run --connection {CONNECTION}",
        "dbt run (refresh all 72 models)",
    )

    # Step 3: Tests (validate data quality)
    if include_tests:
        results["test"] = run_command(
            f"{SNOW_CLI} dbt execute {DBT_PROJECT} test --connection {CONNECTION}",
            "dbt test (validate data quality)",
        )

    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("-" * 70)
    logger.info(f"Pipeline Summary (completed in {elapsed:.0f}s):")
    for step, success in results.items():
        status = "PASS" if success else "FAIL"
        logger.info(f"  {step:10s} : {status}")
    logger.info("=" * 70)

    all_passed = all(results.values())
    if not all_passed:
        logger.warning("One or more steps failed. Check logs for details.")

    return all_passed


def run_scheduler():
    """Run the scheduler loop — checks every 30 seconds if it's time to run."""
    logger.info("dbt Scheduler Started")
    logger.info(f"Scheduled times: {', '.join(SCHEDULE_TIMES)}")
    logger.info(f"Log file: {log_file}")
    logger.info("Press Ctrl+C to stop.\n")

    executed_today = set()

    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        current_date = now.strftime("%Y-%m-%d")

        # Reset tracking at midnight
        if not any(current_date in key for key in executed_today):
            executed_today.clear()

        # Check if current time matches any schedule
        for schedule_time in SCHEDULE_TIMES:
            run_key = f"{current_date}_{schedule_time}"
            if current_time == schedule_time and run_key not in executed_today:
                logger.info(f"Scheduled run triggered at {schedule_time}")
                executed_today.add(run_key)
                run_dbt_pipeline(include_seeds=False, include_tests=True)

        time.sleep(30)  # Check every 30 seconds


def install_windows_tasks():
    """Install Windows Task Scheduler jobs for the three daily runs."""
    script_path = os.path.abspath(__file__)
    python_path = sys.executable

    tasks = [
        ("dbt_run_1030", "10:30", "Morning dbt pipeline refresh"),
        ("dbt_run_1300", "13:00", "Midday dbt pipeline refresh"),
        ("dbt_run_1600", "16:00", "Afternoon dbt pipeline refresh"),
    ]

    logger.info("Installing Windows Task Scheduler jobs...")

    for task_name, run_time, description in tasks:
        cmd = (
            f'schtasks /create /tn "{task_name}" '
            f'/tr "\"{python_path}\" \"{script_path}\" --run-now" '
            f'/sc daily /st {run_time} '
            f'/f /rl HIGHEST '
            f'/ru "{os.environ.get("USERNAME", "SYSTEM")}"'
        )

        success = run_command(cmd, f"Create task: {task_name} at {run_time}")
        if success:
            logger.info(f"  Installed: {task_name} -> daily at {run_time} ({description})")
        else:
            logger.error(f"  Failed to install: {task_name}")
            logger.info(f"  Manual command:\n    {cmd}")

    logger.info("\nTo view installed tasks:")
    logger.info('  schtasks /query /tn "dbt_run_*"')
    logger.info("\nTo remove tasks:")
    for task_name, _, _ in tasks:
        logger.info(f'  schtasks /delete /tn "{task_name}" /f')


def main():
    parser = argparse.ArgumentParser(
        description="Scheduled dbt pipeline runner for Snowflake Cost Optimisation Framework"
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run the pipeline once immediately and exit",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Include seeds in the run (normally skipped for daily refreshes)",
    )
    parser.add_argument(
        "--install",
        action="store_true",
        help="Install as Windows Task Scheduler jobs (requires admin)",
    )
    parser.add_argument(
        "--no-tests",
        action="store_true",
        help="Skip dbt tests for faster runs",
    )

    args = parser.parse_args()

    if args.install:
        install_windows_tasks()
    elif args.run_now:
        success = run_dbt_pipeline(
            include_seeds=args.full,
            include_tests=not args.no_tests,
        )
        sys.exit(0 if success else 1)
    else:
        try:
            run_scheduler()
        except KeyboardInterrupt:
            logger.info("\nScheduler stopped by user.")
            sys.exit(0)


if __name__ == "__main__":
    main()
