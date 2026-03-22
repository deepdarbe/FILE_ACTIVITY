@echo off
chcp 65001 >nul 2>&1
echo.
echo  FILE ACTIVITY - Update Script v2
echo  ========================================
echo.

set INSTALL_DIR=C:\FileActivity

REM Check admin
net session >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Administrator rights required!
    pause
    exit /b 1
)

REM 1. Stop service if running
echo [1/7] Stopping service...
sc query FileActivityService >nul 2>&1
if not errorlevel 1 (
    net stop FileActivityService >nul 2>&1
    echo   Service stopped.
    set SERVICE_EXISTS=1
) else (
    echo   Service not found, skipping.
    set SERVICE_EXISTS=0
)

REM 2. Stop dashboard if running
echo [2/7] Stopping dashboard...
taskkill /f /im FileActivity.exe >nul 2>&1
timeout /t 2 /nobreak >nul

REM 3. Protect data directory (CRITICAL - never delete!)
echo [3/7] Protecting database and user data...
if exist "%INSTALL_DIR%\data\file_activity.db" (
    echo   [OK] Database found: %INSTALL_DIR%\data\file_activity.db
    echo   [OK] Database will NOT be touched during update.
) else (
    echo   [INFO] No existing database found. Fresh install.
)
REM Also protect config, logs, reports
if exist "%INSTALL_DIR%\config\config.yaml" (
    echo   [OK] Config preserved: config\config.yaml
)

REM 4. Backup old bin ONLY (not data, config, logs)
echo [4/7] Backing up old EXE version...
if exist "%INSTALL_DIR%\bin_old" rmdir /s /q "%INSTALL_DIR%\bin_old"
if exist "%INSTALL_DIR%\bin" (
    ren "%INSTALL_DIR%\bin" bin_old
    echo   Old version backed up to bin_old\
)

REM 5. Copy new bin
echo [5/7] Installing new EXE version...
if exist "bin" (
    xcopy /s /e /q /y "bin\*" "%INSTALL_DIR%\bin\" >nul
    echo   New EXE version installed.
) else (
    echo [ERROR] bin\ folder not found in current directory!
    echo   Run this script from the extracted ZIP folder.
    pause
    exit /b 1
)

REM 6. Update scripts (but NOT config or data)
echo [6/7] Updating scripts...
if exist "scripts" (
    xcopy /s /e /q /y "scripts\*" "%INSTALL_DIR%\scripts\" >nul
    echo   Scripts updated.
)
REM Copy update.bat itself for next time
copy /y "update.bat" "%INSTALL_DIR%\update.bat" >nul 2>&1

REM 7. Restart service or dashboard
echo [7/7] Starting...
if "%SERVICE_EXISTS%"=="1" (
    net start FileActivityService
    echo   Service restarted.
) else (
    echo   No service found. Start manually:
    echo     cd %INSTALL_DIR%
    echo     start_dashboard.cmd
)

echo.
echo  ========================================
echo   Update Complete!
echo.
echo   PROTECTED (not changed):
echo     - data\file_activity.db (database)
echo     - config\config.yaml (settings)
echo     - logs\ (log files)
echo     - reports\ (generated reports)
echo.
echo   UPDATED:
echo     - bin\ (new EXE and libraries)
echo     - scripts\ (utility scripts)
echo.
echo   Old EXE: %INSTALL_DIR%\bin_old\
echo   Rollback: ren bin bin_new ^& ren bin_old bin
echo  ========================================
echo.
pause
