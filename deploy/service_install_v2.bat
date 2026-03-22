@echo off
echo.
echo  FILE ACTIVITY - Service Installer
echo  ========================================
echo.

set INSTALL_DIR=C:\FileActivity

REM Check admin
net session >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Administrator rights required!
    echo Right-click and select "Run as administrator"
    pause
    exit /b 1
)

echo [1/3] Installing Windows Service...
sc create FileActivityService ^
    binPath= "\"%INSTALL_DIR%\bin\FileActivity.exe\" service" ^
    start= auto ^
    DisplayName= "FILE ACTIVITY - File Share Monitor" ^
    obj= "LocalSystem"

if errorlevel 1 (
    echo [ERROR] Service installation failed!
    pause
    exit /b 1
)

echo [2/3] Setting service description...
sc description FileActivityService "Windows File Share Analysis, Monitoring and Archiving Service - Automatic file watching, scheduled scans, and dashboard."

echo [3/3] Setting recovery options (auto-restart on failure)...
sc failure FileActivityService reset= 86400 actions= restart/60000/restart/120000/restart/300000

echo.
echo  ========================================
echo   Service Installed Successfully!
echo.
echo   Name:    FileActivityService
echo   Status:  Installed (not started)
echo   Start:   Automatic (on boot)
echo.
echo   Commands:
echo     net start FileActivityService
echo     net stop FileActivityService
echo     sc delete FileActivityService
echo  ========================================
echo.

set /p START="Start service now? (Y/N): "
if /i "%START%"=="Y" (
    net start FileActivityService
    echo Service started!
)
pause
