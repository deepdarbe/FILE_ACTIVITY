@echo off
REM ═══════════════════════════════════════════════════
REM  FILE ACTIVITY - Kaldir
REM ═══════════════════════════════════════════════════

echo.
echo  FILE ACTIVITY Kaldirma
echo  ─────────────────────
echo.

net session >nul 2>&1
if errorlevel 1 (
    echo [HATA] Yonetici olarak calistiriniz.
    pause
    exit /b 1
)

set INSTALL_DIR=C:\FileActivity
set /p INSTALL_DIR="Kurulum dizini [%INSTALL_DIR%]: "

if not exist "%INSTALL_DIR%" (
    echo Kurulum bulunamadi: %INSTALL_DIR%
    pause
    exit /b 1
)

echo.
echo UYARI: %INSTALL_DIR% dizini ve tum icerik silinecek.
echo SQLite veritabani da silinecek (data\file_activity.db).
echo.
set /p CONFIRM="Devam etmek istiyor musunuz? (E/H): "
if /I not "%CONFIRM%"=="E" (
    echo Iptal edildi.
    pause
    exit /b 0
)

REM Windows Task Scheduler gorevlerini kaldir
echo [1/4] Zamanlanmis gorevler kaldiriliyor...
for /f "tokens=1 delims=," %%t in ('schtasks /Query /FO CSV /NH 2^>nul ^| findstr FileActivity') do (
    schtasks /Delete /TN "%%~t" /F >nul 2>&1
)

REM Windows servisi durdur
echo [2/4] Servis durduruluyor...
sc stop FileActivityDashboard >nul 2>&1
sc delete FileActivityDashboard >nul 2>&1

REM Firewall kurali
echo [3/4] Firewall kurali kaldiriliyor...
netsh advfirewall firewall delete rule name="FileActivity Dashboard" >nul 2>&1

REM Dosyalari sil
echo [4/4] Dosyalar siliniyor...
rmdir /s /q "%INSTALL_DIR%"

echo.
echo Kaldirma tamamlandi.
echo Not: Veritabani yedegi almak isterseniz: data\file_activity.db dosyasini kopyalayin.
echo.
pause
