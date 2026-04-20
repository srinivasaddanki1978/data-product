@echo off
REM ============================================================
REM dbt Pipeline Runner - Snowflake Cost Optimisation Framework
REM ============================================================
REM Schedule this in Windows Task Scheduler at:
REM   10:30 AM, 1:00 PM, 4:00 PM
REM
REM Steps to set up Windows Task Scheduler:
REM   1. Open Task Scheduler (taskschd.msc)
REM   2. Create Task > Name: "dbt_refresh_1030"
REM   3. Trigger > Daily > Start at 10:30 AM
REM   4. Action > Start a Program > Browse to this .bat file
REM   5. Repeat for 1:00 PM and 4:00 PM
REM ============================================================

set PROJECT_DIR=C:\Srinivas\project\data-product
set LOG_DIR=%PROJECT_DIR%\logs
set CONNECTION=cost_optimization
set DBT_PROJECT=cost_optimization

REM Create log directory if needed
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Log file with date
for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set LOGDATE=%%c%%a%%b
set LOG_FILE=%LOG_DIR%\dbt_run_%LOGDATE%_%TIME:~0,2%%TIME:~3,2%.log

echo ============================================================ >> "%LOG_FILE%"
echo dbt Pipeline Run: %DATE% %TIME% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

cd /d %PROJECT_DIR%

echo [%TIME%] Running: dbt run >> "%LOG_FILE%"
snow dbt execute %DBT_PROJECT% run --connection %CONNECTION% >> "%LOG_FILE%" 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [%TIME%] dbt run: PASSED >> "%LOG_FILE%"
) else (
    echo [%TIME%] dbt run: FAILED >> "%LOG_FILE%"
)

echo [%TIME%] Running: dbt test >> "%LOG_FILE%"
snow dbt execute %DBT_PROJECT% test --connection %CONNECTION% >> "%LOG_FILE%" 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [%TIME%] dbt test: PASSED >> "%LOG_FILE%"
) else (
    echo [%TIME%] dbt test: FAILED >> "%LOG_FILE%"
)

echo ============================================================ >> "%LOG_FILE%"
echo [%TIME%] Pipeline complete >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
