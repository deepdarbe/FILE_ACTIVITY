@echo off
setlocal EnableDelayedExpansion
REM ===========================================================
REM  FILE ACTIVITY - Server Installation Script (SQLite)
REM  Run as Administrator (Right-click - Run as administrator)
REM ===========================================================

echo.
echo  ==========================================
echo   FILE ACTIVITY - Installation Wizard
echo  ==========================================
echo   Database: SQLite (zero configuration)
echo.

REM --- Admin check ---
net session >nul 2>&1
if errorlevel 1 (
    echo [ERROR] This script must run as Administrator!
    echo         Right-click -- Run as administrator
    pause
    exit /b 1
)

REM --- Detect script location and install mode ---
set SCRIPT_DIR=%~dp0
echo   Script location: %SCRIPT_DIR%
echo.

REM --- Show what we can see ---
echo   Checking files in script directory...
if exist "%SCRIPT_DIR%bin\FileActivity.exe" echo   [FOUND] bin\FileActivity.exe
if exist "%SCRIPT_DIR%bin" echo   [FOUND] bin\ folder
if exist "%SCRIPT_DIR%src" echo   [FOUND] src\ folder
if exist "%SCRIPT_DIR%main.py" echo   [FOUND] main.py
if exist "%SCRIPT_DIR%config" echo   [FOUND] config\ folder
if exist "%SCRIPT_DIR%config\config.yaml" echo   [FOUND] config\config.yaml
echo.

REM --- Detect mode ---
set MODE=NONE

if exist "%SCRIPT_DIR%bin\FileActivity.exe" (
    set MODE=EXE
    echo   Install Mode: EXE Package [Python NOT required]
    goto MODE_DETECTED
)

if exist "%SCRIPT_DIR%src\__init__.py" (
    set MODE=SOURCE
    echo   Install Mode: Source Code [Python required]
    goto MODE_DETECTED
)

if exist "%SCRIPT_DIR%main.py" (
    set MODE=SOURCE
    echo   Install Mode: Source Code [Python required]
    goto MODE_DETECTED
)

REM --- Check one level deeper ---
echo   [WARN] Files not found in current directory.
echo   Checking subdirectories...
echo.

for /d %%d in ("%SCRIPT_DIR%*") do (
    if exist "%%d\bin\FileActivity.exe" (
        set SCRIPT_DIR=%%d\
        set MODE=EXE
        echo   [FOUND] EXE package in: %%d\
        goto MODE_DETECTED
    )
    if exist "%%d\main.py" (
        set SCRIPT_DIR=%%d\
        set MODE=SOURCE
        echo   [FOUND] Source code in: %%d\
        goto MODE_DETECTED
    )
)

echo [ERROR] Installation files not found!
echo.
echo   Expected one of:
echo     %SCRIPT_DIR%bin\FileActivity.exe
echo     %SCRIPT_DIR%src\
echo     %SCRIPT_DIR%main.py
echo.
echo   TIP: Make sure you extracted the ZIP correctly.
echo   Current folder contents:
dir /b "%SCRIPT_DIR%" 2>nul
echo.
pause
exit /b 1

:MODE_DETECTED
echo   Source dir: %SCRIPT_DIR%
echo.

REM --- Python check (only for SOURCE mode) ---
if "%MODE%"=="SOURCE" (
    echo [1/7] Checking Python...
    where python >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Python not found! Install Python 3.10+
        echo         https://www.python.org/downloads/
        pause
        exit /b 1
    )
    for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PY_VER=%%v
    echo   Python: !PY_VER!
) else (
    echo [1/7] Python check... SKIPPED (EXE mode, not needed)
)

REM --- Install directory ---
echo.
set INSTALL_DIR=C:\FileActivity
set /p INSTALL_DIR="  Install directory [%INSTALL_DIR%]: "

echo.
echo [2/7] Creating directory: %INSTALL_DIR%
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
mkdir "%INSTALL_DIR%\config" 2>nul
mkdir "%INSTALL_DIR%\data" 2>nul
mkdir "%INSTALL_DIR%\logs" 2>nul
mkdir "%INSTALL_DIR%\reports" 2>nul
mkdir "%INSTALL_DIR%\scripts" 2>nul

REM --- Copy files ---
echo [3/7] Copying files...
if "%MODE%"=="EXE" (
    xcopy /s /e /q /y "%SCRIPT_DIR%bin\*" "%INSTALL_DIR%\bin\" >nul 2>&1
    if exist "%SCRIPT_DIR%config\config.yaml" (
        copy /y "%SCRIPT_DIR%config\config.yaml" "%INSTALL_DIR%\config\" >nul 2>&1
    )
    if exist "%SCRIPT_DIR%scripts\init_db.py" (
        copy /y "%SCRIPT_DIR%scripts\init_db.py" "%INSTALL_DIR%\scripts\" >nul 2>&1
    )
    echo   EXE package copied to %INSTALL_DIR%\bin\
) else (
    xcopy /s /e /q /y "%SCRIPT_DIR%src" "%INSTALL_DIR%\src\" >nul 2>&1
    copy /y "%SCRIPT_DIR%main.py" "%INSTALL_DIR%\" >nul 2>&1
    if exist "%SCRIPT_DIR%dev_server.py" copy /y "%SCRIPT_DIR%dev_server.py" "%INSTALL_DIR%\" >nul 2>&1
    if exist "%SCRIPT_DIR%config.yaml" copy /y "%SCRIPT_DIR%config.yaml" "%INSTALL_DIR%\config\" >nul 2>&1
    if exist "%SCRIPT_DIR%config\config.yaml" copy /y "%SCRIPT_DIR%config\config.yaml" "%INSTALL_DIR%\config\" >nul 2>&1
    if exist "%SCRIPT_DIR%requirements.txt" copy /y "%SCRIPT_DIR%requirements.txt" "%INSTALL_DIR%\" >nul 2>&1
    echo   Source code copied.
)

REM --- Dependencies (SOURCE mode only) ---
echo [4/7] Dependencies...
if "%MODE%"=="SOURCE" (
    python -m pip install -r "%INSTALL_DIR%\requirements.txt" -q 2>nul
    echo   Python dependencies installed.
) else (
    echo   EXE mode - no external dependencies needed.
    echo   Database: SQLite (built-in, zero config)
)

REM --- PORT CHECK ---
echo.
echo [5/7] Dashboard port check...
set DASH_PORT=8085

:PORT_CHECK
netstat -an | findstr ":%DASH_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [!] Port %DASH_PORT% is in use!
    echo.
    echo   Available ports:
    call :FIND_FREE_PORTS
    echo.
    set /p DASH_PORT="  Select dashboard port [%DASH_PORT%]: "
    goto PORT_CHECK
) else (
    echo   [OK] Port %DASH_PORT% is available.
)

REM --- Update config.yaml ---
echo [6/7] Updating configuration...

REM SQLite: just set the database path and dashboard port
set CFG_FILE=%INSTALL_DIR%\config\config.yaml
set DB_PATH=%INSTALL_DIR%\data\file_activity.db

REM Use Python if available (cleanest), fallback to PowerShell
set CFG_UPDATED=0

where python >nul 2>&1
if not errorlevel 1 (
    python -c "p='%CFG_FILE%'.replace('\\\\','/');c=open(p,'r',encoding='utf-8').read();c=c.replace('data/file_activity.db','%DB_PATH:\=/%');c=c.replace('port: 8085','port: %DASH_PORT%');open(p,'w',encoding='utf-8').write(c);print('  config.yaml updated.')" 2>nul
    if not errorlevel 1 set CFG_UPDATED=1
)

if "!CFG_UPDATED!"=="0" (
    where powershell >nul 2>&1
    if not errorlevel 1 (
        powershell -NoProfile -Command "$c=Get-Content '%CFG_FILE%' -Raw;$c=$c.Replace('data/file_activity.db','%DB_PATH:\=/%');$c=$c.Replace('port: 8085','port: %DASH_PORT%');Set-Content '%CFG_FILE%' $c;Write-Host '  config.yaml updated.'"
    ) else (
        echo   [WARN] Could not auto-update config. Edit manually:
        echo          %CFG_FILE%
    )
)

REM --- Create launcher scripts ---
echo   Creating launcher scripts...

if "%MODE%"=="EXE" (
    set FA_CMD=%INSTALL_DIR%\bin\FileActivity.exe
) else (
    set FA_CMD=python "%INSTALL_DIR%\main.py"
)

REM Quick launcher: fa.cmd
(
echo @echo off
echo REM FILE ACTIVITY Quick Launcher
echo !FA_CMD! --config "%INSTALL_DIR%\config\config.yaml" %%*
) > "%INSTALL_DIR%\fa.cmd"

REM Dashboard launcher
(
echo @echo off
echo echo Starting FILE ACTIVITY Dashboard on port %DASH_PORT%...
echo !FA_CMD! --config "%INSTALL_DIR%\config\config.yaml" dashboard
echo pause
) > "%INSTALL_DIR%\start_dashboard.cmd"

echo   Created: fa.cmd, start_dashboard.cmd

REM --- System PATH ---
echo [7/7] Adding to system PATH...
setx /M PATH "%PATH%;%INSTALL_DIR%" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] %INSTALL_DIR% added to PATH.
) else (
    echo   [WARN] Could not update PATH. Add manually.
)

REM --- Firewall ---
echo   Adding firewall rule (port %DASH_PORT%)...
netsh advfirewall firewall delete rule name="FileActivity Dashboard" >nul 2>&1
netsh advfirewall firewall add rule name="FileActivity Dashboard" dir=in action=allow protocol=tcp localport=%DASH_PORT% >nul 2>&1
if not errorlevel 1 (
    echo   [OK] Firewall rule added.
) else (
    echo   [WARN] Could not add firewall rule.
)

REM --- Quick DB test ---
echo.
echo --- Database Test ---
echo   SQLite database will be created automatically at:
echo   %DB_PATH%
echo   No external database server needed!

REM --- Verify installation ---
echo.
echo --- Installation Verification ---
set VERIFY_PASS=0
set VERIFY_FAIL=0

if exist "%INSTALL_DIR%\config\config.yaml" (
    echo   [OK] config\config.yaml
    set /a VERIFY_PASS+=1
) else (
    echo   [MISSING] config\config.yaml
    set /a VERIFY_FAIL+=1
)

if exist "%INSTALL_DIR%\data" (
    echo   [OK] data\ directory
    set /a VERIFY_PASS+=1
) else (
    echo   [MISSING] data\
    set /a VERIFY_FAIL+=1
)

if "%MODE%"=="EXE" (
    if exist "%INSTALL_DIR%\bin\FileActivity.exe" (
        echo   [OK] bin\FileActivity.exe
        set /a VERIFY_PASS+=1
    ) else (
        echo   [MISSING] bin\FileActivity.exe
        set /a VERIFY_FAIL+=1
    )
) else (
    if exist "%INSTALL_DIR%\main.py" (
        echo   [OK] main.py
        set /a VERIFY_PASS+=1
    ) else (
        echo   [MISSING] main.py
        set /a VERIFY_FAIL+=1
    )
)

if exist "%INSTALL_DIR%\fa.cmd" (
    echo   [OK] fa.cmd launcher
    set /a VERIFY_PASS+=1
) else (
    echo   [MISSING] fa.cmd
    set /a VERIFY_FAIL+=1
)

if exist "%INSTALL_DIR%\logs" (
    echo   [OK] logs\ directory
    set /a VERIFY_PASS+=1
) else (
    echo   [MISSING] logs\
    set /a VERIFY_FAIL+=1
)

echo.
echo   Result: !VERIFY_PASS! passed, !VERIFY_FAIL! failed
echo.

REM --- Summary ---
echo  ==========================================
echo   Installation Complete!
echo  ==========================================
echo.
echo   Directory:  %INSTALL_DIR%
echo   Mode:       %MODE%
echo   Database:   SQLite (%DB_PATH%)
echo   Dashboard:  http://localhost:%DASH_PORT%
echo.
echo   --- Quick Start ---
echo.
echo   1. Add a file share source:
echo      cd %INSTALL_DIR%
echo      fa source add -n SERVER01 -p \\server\share -a \\archive\dest
echo.
echo   2. Run first scan:
echo      fa scan -s SERVER01
echo.
echo   3. Start dashboard:
echo      start_dashboard.cmd
echo.
echo   4. Check system status:
echo      fa check
echo.
pause
exit /b 0

REM ===========================================================
REM  HELPER FUNCTIONS
REM ===========================================================

:FIND_FREE_PORTS
set FREE_COUNT=0
for %%p in (8080 8081 8082 8083 8084 8085 8086 8087 8088 8089 8090 9000 9090) do (
    netstat -an | findstr ":%%p " | findstr "LISTENING" >nul 2>&1
    if errorlevel 1 (
        if !FREE_COUNT! LSS 5 (
            echo     %%p - available
            set /a FREE_COUNT+=1
        )
    )
)
if !FREE_COUNT!==0 echo     No standard ports available.
goto :eof
