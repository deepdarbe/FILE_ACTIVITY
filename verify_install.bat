@echo off
chcp 65001 >nul 2>&1
echo.
echo  ========================================
echo   FILE ACTIVITY - Kurulum Dogrulama
echo  ========================================
echo.

REM Varsayilan kurulum dizini
set CHECK_DIR=C:\FileActivity
if not "%~1"=="" set CHECK_DIR=%~1

echo [1/7] Kurulum Dizini Kontrolu...
if exist "%CHECK_DIR%" (
    echo        [OK] %CHECK_DIR% mevcut
) else (
    echo        [HATA] %CHECK_DIR% bulunamadi!
    echo.
    echo   install.bat'i Yonetici olarak calistirdiniz mi?
    echo   Sag tikla - "Yonetici olarak calistir"
    echo.
    echo   Veya farkli dizine kurduysaniz:
    echo     verify_install.bat D:\FileActivity
    echo.
    pause
    exit /b 1
)

echo [2/7] Dosya Yapisi Kontrolu...
set PASS=0
set FAIL=0

if exist "%CHECK_DIR%\config\config.yaml" (
    echo        [OK] config\config.yaml
    set /a PASS+=1
) else (
    echo        [EKSIK] config\config.yaml
    set /a FAIL+=1
)

if exist "%CHECK_DIR%\bin\FileActivity.exe" (
    echo        [OK] bin\FileActivity.exe
    set /a PASS+=1
) else if exist "%CHECK_DIR%\main.py" (
    echo        [OK] main.py (kaynak mod)
    set /a PASS+=1
) else (
    echo        [EKSIK] FileActivity.exe veya main.py
    set /a FAIL+=1
)

if exist "%CHECK_DIR%\scripts\init_db.py" (
    echo        [OK] scripts\init_db.py
    set /a PASS+=1
) else (
    echo        [EKSIK] scripts\init_db.py
    set /a FAIL+=1
)

if exist "%CHECK_DIR%\logs" (
    echo        [OK] logs\ dizini
    set /a PASS+=1
) else (
    echo        [EKSIK] logs\ dizini
    set /a FAIL+=1
)

if exist "%CHECK_DIR%\reports" (
    echo        [OK] reports\ dizini
    set /a PASS+=1
) else (
    echo        [EKSIK] reports\ dizini
    set /a FAIL+=1
)

echo.
echo [3/7] Python Kontrolu...
where python >nul 2>&1
if errorlevel 1 (
    echo        [HATA] Python bulunamadi
) else (
    for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo        [OK] %%v
)

echo [4/7] PostgreSQL Kontrolu...
where psql >nul 2>&1
if errorlevel 1 (
    echo        [UYARI] psql bulunamadi (uzak baglanti ile de calisabilir)
) else (
    for /f "tokens=*" %%v in ('psql --version 2^>^&1') do echo        [OK] %%v
)

echo [5/7] Dashboard Port Kontrolu...
REM Config'den port oku
set DASH_PORT=8085
for /f "tokens=2 delims=: " %%p in ('findstr /C:"port:" "%CHECK_DIR%\config\config.yaml" 2^>nul ^| findstr /V "5432"') do (
    set DASH_PORT=%%p
)
netstat -an 2>nul | findstr ":%DASH_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo        [AKTIF] Dashboard portu %DASH_PORT% dinliyor
) else (
    echo        [KAPALI] Dashboard portu %DASH_PORT% dinlemiyor (normal - henuz baslatilmamis)
)

echo [6/7] PostgreSQL Baglanti Testi...
python -c "
import sys
sys.path.insert(0, r'%CHECK_DIR%')
try:
    import yaml
    with open(r'%CHECK_DIR%\config\config.yaml') as f:
        cfg = yaml.safe_load(f)
    db = cfg.get('database', {})
    import psycopg2
    conn = psycopg2.connect(
        host=db.get('host','localhost'),
        port=db.get('port', 5432),
        dbname=db.get('name','file_activity'),
        user=db.get('user','file_activity'),
        password=db.get('password',''),
        connect_timeout=5
    )
    conn.close()
    print('       [OK] PostgreSQL baglantisi basarili')
except ImportError as e:
    print(f'       [UYARI] Modul eksik: {e}')
except Exception as e:
    print(f'       [HATA] PostgreSQL: {e}')
" 2>nul

echo [7/7] Firewall Kurali Kontrolu...
netsh advfirewall firewall show rule name="FileActivity Dashboard" >nul 2>&1
if not errorlevel 1 (
    echo        [OK] Firewall kurali mevcut
) else (
    echo        [EKSIK] Firewall kurali yok (Yonetici olarak kurulum gerekli)
)

echo.
echo  ========================================
echo   Sonuc: %PASS% basarili, %FAIL% eksik
echo  ========================================
echo.

if %FAIL% GTR 0 (
    echo   [!] Eksik ogeler var. install.bat'i Yonetici olarak tekrar calistirin.
) else (
    echo   [OK] Kurulum dogru gorunuyor!
    echo.
    echo   Siradaki adimlar:
    echo     1. Veritabani olustur:
    echo        cd %CHECK_DIR%
    echo        python scripts\init_db.py
    echo.
    echo     2. Kaynak ekle:
    echo        python main.py source add -n SRV01 -p \\server\share -a \\archive\dest
    echo.
    echo     3. Ilk tarama:
    echo        python main.py scan -s SRV01
    echo.
    echo     4. Dashboard baslat:
    echo        python main.py dashboard
)
echo.
pause
