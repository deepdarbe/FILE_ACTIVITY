@echo off
REM ═══════════════════════════════════════════════════
REM  FILE ACTIVITY - Windows Servisi Olarak Kur
REM  Dashboard'u arka planda Windows servisi olarak calistirir.
REM  Gereksinim: NSSM (Non-Sucking Service Manager)
REM ═══════════════════════════════════════════════════

echo.
echo  FILE ACTIVITY - Servis Kurulumu
echo  ────────────────────────────────
echo.

net session >nul 2>&1
if errorlevel 1 (
    echo [HATA] Yonetici olarak calistiriniz.
    pause
    exit /b 1
)

set INSTALL_DIR=C:\FileActivity
set /p INSTALL_DIR="Kurulum dizini [%INSTALL_DIR%]: "

set SVC_NAME=FileActivityDashboard

REM NSSM kontrolu
where nssm >nul 2>&1
if errorlevel 1 (
    echo.
    echo NSSM bulunamadi. Kurulum icin:
    echo   winget install nssm
    echo   veya: choco install nssm
    echo   veya: https://nssm.cc/download
    echo.
    echo NSSM olmadan alternatif yontem: Windows Task Scheduler
    echo.

    echo [*] Task Scheduler ile baslatma gorevi olusturuluyor...
    schtasks /Create /TN "FileActivity_Dashboard" ^
        /TR "\"%INSTALL_DIR%\bin\FileActivity.exe\" dashboard --config \"%INSTALL_DIR%\config\config.yaml\"" ^
        /SC ONSTART /RU SYSTEM /F

    if errorlevel 1 (
        echo [HATA] Gorev olusturulamadi.
    ) else (
        echo [OK] Task Scheduler gorevi olusturuldu.
        echo     Sistem baslandiginda dashboard otomatik baslar.
    )
    pause
    exit /b 0
)

REM NSSM ile servis kur
echo [1/3] Servis olusturuluyor: %SVC_NAME%
nssm install %SVC_NAME% "%INSTALL_DIR%\bin\FileActivity.exe"
nssm set %SVC_NAME% AppParameters "dashboard --config \"%INSTALL_DIR%\config\config.yaml\""
nssm set %SVC_NAME% AppDirectory "%INSTALL_DIR%"

echo [2/3] Servis ayarlari yapiliyor...
nssm set %SVC_NAME% DisplayName "FILE ACTIVITY Dashboard"
nssm set %SVC_NAME% Description "Dosya Paylasim Analiz ve Arsivleme Sistemi - Web Dashboard"
nssm set %SVC_NAME% Start SERVICE_AUTO_START
nssm set %SVC_NAME% AppStdout "%INSTALL_DIR%\logs\dashboard_stdout.log"
nssm set %SVC_NAME% AppStderr "%INSTALL_DIR%\logs\dashboard_stderr.log"
nssm set %SVC_NAME% AppRotateFiles 1
nssm set %SVC_NAME% AppRotateBytes 10485760

echo [3/3] Servis baslatiliyor...
nssm start %SVC_NAME%

echo.
echo  ╔══════════════════════════════════════════╗
echo  ║  Servis kuruldu ve baslatildi!           ║
echo  ║                                          ║
echo  ║  Ad:    %SVC_NAME%
echo  ║  URL:   http://localhost:8085            ║
echo  ║                                          ║
echo  ║  Yonetim komutlari:                      ║
echo  ║    nssm start %SVC_NAME%
echo  ║    nssm stop  %SVC_NAME%
echo  ║    nssm restart %SVC_NAME%
echo  ║    nssm status  %SVC_NAME%
echo  ╚══════════════════════════════════════════╝
echo.
pause
