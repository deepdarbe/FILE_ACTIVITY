@echo off
chcp 65001 >nul 2>&1
REM ═══════════════════════════════════════════════════
REM  FILE ACTIVITY - EXE Build Script
REM  Kullanim: build.bat
REM ═══════════════════════════════════════════════════

echo.
echo  FILE ACTIVITY - Build Process
echo  ========================================
echo.

REM Python bul
where python >nul 2>&1
if errorlevel 1 (
    echo [HATA] Python bulunamadi! PATH'e ekleyin.
    pause
    exit /b 1
)
for /f "tokens=*" %%p in ('python -c "import sys; print(sys.executable)"') do set PYTHON=%%p
echo   Python: %PYTHON%

REM pip ve pyinstaller icin python -m kullan (PATH sorunu olmaz)
echo [1/5] PyInstaller kontrol ediliyor...
python -m pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo   PyInstaller yukleniyor...
    python -m pip install pyinstaller
)

echo [2/5] Bagimliliklar kontrol ediliyor...
python -m pip install -r requirements.txt -q

REM Build
echo [3/5] EXE olusturuluyor...
python -m PyInstaller file_activity.spec --noconfirm

if errorlevel 1 (
    echo.
    echo [HATA] Build basarisiz!
    pause
    exit /b 1
)

REM Deployment paketi olustur
echo [4/5] Dagitim paketi hazirlaniyor...

set DIST=dist\FileActivity
set PKG=dist\FileActivity-Package

if exist "%PKG%" rmdir /s /q "%PKG%"
mkdir "%PKG%"
mkdir "%PKG%\bin"
mkdir "%PKG%\config"
mkdir "%PKG%\scripts"
mkdir "%PKG%\logs"
mkdir "%PKG%\reports"

REM EXE ve bagimliliklar
xcopy /s /e /q "%DIST%\*" "%PKG%\bin\" >nul

REM Config template
copy config.yaml "%PKG%\config\config.yaml" >nul

REM Kurulum scriptleri
copy scripts\init_db.py "%PKG%\scripts\" >nul
copy deploy\install.bat "%PKG%\install.bat" >nul
copy deploy\uninstall.bat "%PKG%\uninstall.bat" >nul
copy deploy\service_install.bat "%PKG%\service_install.bat" >nul

REM ZIP paketi
echo [5/5] ZIP paketi olusturuluyor...
python pack.py --output "dist\FileActivity-Package.zip"

echo.
echo  ========================================
echo   Build Tamamlandi!
echo.
echo   EXE Paket: dist\FileActivity-Package\
echo   ZIP Paket: dist\FileActivity-Package.zip
echo  ========================================
echo.
pause
