<#
.SYNOPSIS
    FILE ACTIVITY - Tek Komutla Kaynak Kod Kurulumu (master branch)

.DESCRIPTION
    GitHub master branch'inden kaynak kodu indirir, Python venv olusturur,
    bagimliliklari (duckdb dahil) kurar, launcher scriptleri hazirlar ve
    istege bagli olarak dashboard'u baslatir.

    EXE release'i GEREKTIRMEZ. Tek gereksinim: hedef sunucuda Python 3.10+

    Kullanim (Yonetici PowerShell):
    powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup-source.ps1 | iex"

    Bastaki TLS 1.2 atamasi, eski PowerShell 5.1 (Windows Server 2012/2016)
    varsayili TLS 1.0/1.1 kullandigi icin GitHub'a HTTPS isteginin calismasini
    garanti eder. Daha yeni sistemlerde zararsiz, guvenli tarafta kalmak icin
    kanonik komut olarak onerilir.

    Ayni komut guncelleme icin de kullanilabilir: mevcut data\, config\,
    logs\ ve reports\ dizinleri korunur, sadece kaynak kod yenilenir.

.NOTES
    Veri korumali guncelleme: data/, logs/, reports/, config/config.yaml
    Yeniden yazilir: src/, main.py, requirements.txt, deploy/, scripts/
#>

$ErrorActionPreference = "Stop"

# --- Konfigurasyon ---
$InstallDir   = "C:\FileActivity"
$RepoOwner    = "deepdarbe"
$RepoName     = "FILE_ACTIVITY"
$Branch       = "master"
$RepoZipUrl   = "https://github.com/$RepoOwner/$RepoName/archive/refs/heads/$Branch.zip"
$DashPort     = 8085
$PythonVersion = "3.11.9"
$PythonUrl    = "https://www.python.org/ftp/python/$PythonVersion/python-$PythonVersion-amd64.exe"

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host "  |  FILE ACTIVITY - Kaynak Kod Kurulumu     |" -ForegroundColor Cyan
Write-Host "  |  $RepoOwner/$RepoName@$Branch            |" -ForegroundColor Cyan
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host ""

# --- Yonetici kontrolu ---
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "  [HATA] Bu script Yonetici olarak calistirilmalidir." -ForegroundColor Red
    Write-Host "  PowerShell'i sag tikla -> 'Yonetici olarak calistir', sonra komutu tekrar ver." -ForegroundColor Yellow
    exit 1
}

# --- TLS (Python installer indirme icin erken aktiflestir) ---
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Find-Python {
    foreach ($candidate in @("python", "py -3")) {
        try {
            $out = & cmd /c "$candidate --version 2>&1"
            if ($LASTEXITCODE -eq 0 -and $out -match "Python (\d+)\.(\d+)") {
                $major = [int]$Matches[1]; $minor = [int]$Matches[2]
                if ($major -eq 3 -and $minor -ge 10) {
                    return @{ Command = $candidate; Version = $out.Trim() }
                }
            }
        } catch {}
    }
    # Son care: yaygin kurulum yollarinda ara (PATH henuz yenilenmemis olabilir)
    $commonPaths = @(
        "$env:ProgramFiles\Python311\python.exe",
        "$env:ProgramFiles\Python312\python.exe",
        "$env:ProgramFiles\Python310\python.exe",
        "$env:LocalAppData\Programs\Python\Python311\python.exe",
        "$env:LocalAppData\Programs\Python\Python312\python.exe"
    )
    foreach ($p in $commonPaths) {
        if (Test-Path $p) {
            $out = & $p --version 2>&1
            if ($out -match "Python (\d+)\.(\d+)" -and [int]$Matches[1] -eq 3 -and [int]$Matches[2] -ge 10) {
                return @{ Command = "`"$p`""; Version = $out.Trim() }
            }
        }
    }
    return $null
}

function Refresh-Path {
    $m = [System.Environment]::GetEnvironmentVariable("Path", "Machine")
    $u = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$m;$u"
}

# --- 1. Python kontrolu (yoksa otomatik kur) ---
Write-Host "[1/6] Python 3.10+ kontrol ediliyor..." -ForegroundColor Yellow
$py = Find-Python

if (-not $py) {
    Write-Host "  Python 3.10+ bulunamadi. Otomatik kurulum baslatiliyor..." -ForegroundColor Yellow
    Write-Host "  Python $PythonVersion indiriliyor (~28 MB)..." -ForegroundColor Gray

    $pyInstaller = "$env:TEMP\python-$PythonVersion-amd64.exe"
    try {
        Invoke-WebRequest -Uri $PythonUrl -OutFile $pyInstaller -UseBasicParsing
    } catch {
        Write-Host "  [HATA] Python installer indirilemedi: $_" -ForegroundColor Red
        Write-Host "         Manuel: https://www.python.org/downloads/" -ForegroundColor Yellow
        exit 1
    }

    Write-Host "  Python sessiz kuruluyor (tum kullanicilar, PATH'e eklenir)..." -ForegroundColor Gray
    $pyArgs = @(
        "/quiet",
        "InstallAllUsers=1",
        "PrependPath=1",
        "Include_test=0",
        "Include_launcher=1",
        "Include_pip=1"
    )
    $proc = Start-Process -FilePath $pyInstaller -ArgumentList $pyArgs -Wait -PassThru
    Remove-Item $pyInstaller -Force -ErrorAction SilentlyContinue

    if ($proc.ExitCode -ne 0) {
        Write-Host "  [HATA] Python kurulumu basarisiz (exit $($proc.ExitCode))" -ForegroundColor Red
        exit 1
    }

    # Mevcut PowerShell oturumunda yeni PATH'i gor
    Refresh-Path
    $py = Find-Python

    if (-not $py) {
        Write-Host "  [HATA] Python kuruldu ama calistirilabilir bulunamadi." -ForegroundColor Red
        Write-Host "         PowerShell'i kapatip yeniden acin ve komutu tekrar calistirin." -ForegroundColor Yellow
        exit 1
    }
    Write-Host "  [OK] Python kuruldu: $($py.Version)" -ForegroundColor Green
} else {
    Write-Host "  [OK] $($py.Version) -> komut: $($py.Command)" -ForegroundColor Green
}
$pythonCmd = $py.Command

# --- 2. Kaynak kodu indir ---
Write-Host "[2/6] Kaynak kod indiriliyor ($Branch)..." -ForegroundColor Yellow
$zipPath     = "$env:TEMP\fileactivity-$Branch.zip"
$extractPath = "$env:TEMP\fileactivity-$Branch-extract"
if (Test-Path $zipPath)     { Remove-Item $zipPath -Force }
if (Test-Path $extractPath) { Remove-Item $extractPath -Recurse -Force }

try {
    Invoke-WebRequest -Uri $RepoZipUrl -OutFile $zipPath -UseBasicParsing
} catch {
    Write-Host "  [HATA] Indirme basarisiz: $_" -ForegroundColor Red
    exit 1
}
$dlSize = [math]::Round((Get-Item $zipPath).Length / 1MB, 1)
Write-Host "  [OK] $dlSize MB indirildi" -ForegroundColor Green

# --- 3. Dizin yapisi + kaynak kopyala ---
Write-Host "[3/6] Kurulum dizini: $InstallDir" -ForegroundColor Yellow

# Calisan process varsa durdur (guncelleme sirasinda dosya kilidi olmasin)
Get-Process -Name "python","pythonw" -ErrorAction SilentlyContinue | Where-Object {
    try { $_.Path -and $_.Path.StartsWith($InstallDir) } catch { $false }
} | ForEach-Object {
    Write-Host "  Calisan process durduruluyor: PID $($_.Id)" -ForegroundColor Gray
    $_ | Stop-Process -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Seconds 1

Expand-Archive -Path $zipPath -DestinationPath $extractPath -Force
$srcRoot = (Get-ChildItem $extractPath -Directory | Select-Object -First 1).FullName

# COMMIT_SHA yaz — GitHub API'den master head SHA al ve srcRoot'a dusur.
# Bu sayede VERSION ayni kalsa bile dashboard'da gercek commit gorunur
# (ornek: 1.8.0-dev+a1b2c3d). API unavailable ise sessizce atla.
try {
    $apiUrl = "https://api.github.com/repos/$RepoOwner/$RepoName/commits/$Branch"
    $headSha = (Invoke-RestMethod -Uri $apiUrl -UseBasicParsing -Headers @{
        "User-Agent" = "file-activity-installer"
    }).sha
    if ($headSha) {
        Set-Content -Path "$srcRoot\COMMIT_SHA" -Value $headSha.Substring(0, 7) -NoNewline
        Write-Host "  [OK] Commit: $($headSha.Substring(0, 7))" -ForegroundColor DarkGray
    }
} catch {
    # Kurumsal ag GitHub API'sine izin vermeyebilir — sorun degil
}

# Korumali dizinler (guncelleme senaryosunda data kaybolmasin)
$preserveDirs = @("data", "logs", "reports", ".venv")
$existingConfig = Test-Path "$InstallDir\config\config.yaml"

foreach ($d in @("", "\data", "\logs", "\reports", "\config")) {
    $p = "$InstallDir$d"
    if (-not (Test-Path $p)) { New-Item -Path $p -ItemType Directory -Force | Out-Null }
}

# Koddan gelen uzerine yazilmamasi gereken top-level itemlar
$skipTop = @("data", "logs", "reports", ".git", ".github", "dist", "build", ".venv")

Get-ChildItem $srcRoot -Force | Where-Object { $skipTop -notcontains $_.Name } | ForEach-Object {
    $dest = "$InstallDir\$($_.Name)"
    if ($_.PSIsContainer) {
        if (Test-Path $dest) { Remove-Item $dest -Recurse -Force }
        Copy-Item -Path $_.FullName -Destination $dest -Recurse -Force
    } else {
        Copy-Item -Path $_.FullName -Destination $dest -Force
    }
}

# config.yaml: sadece yoksa olustur, varsa dokunma
if (-not $existingConfig) {
    Copy-Item "$srcRoot\config.yaml" "$InstallDir\config\config.yaml" -Force
    $cfg = Get-Content "$InstallDir\config\config.yaml" -Raw
    $dbPath = ("$InstallDir/data/file_activity.db" -replace '\\','/')
    $cfg = $cfg -replace 'path: "data/file_activity.db"', "path: `"$dbPath`""
    Set-Content "$InstallDir\config\config.yaml" $cfg
    Write-Host "  [OK] config\config.yaml olusturuldu (ilk kurulum)" -ForegroundColor Green
} else {
    Write-Host "  [OK] config\config.yaml korundu (mevcut ayarlar)" -ForegroundColor Green
}

# --- 4. Venv + bagimliliklar ---
Write-Host "[4/6] Python sanal ortam + bagimliliklar..." -ForegroundColor Yellow
$venvPath = "$InstallDir\.venv"
$venvPy   = "$venvPath\Scripts\python.exe"

if (-not (Test-Path $venvPy)) {
    & cmd /c "$pythonCmd -m venv `"$venvPath`""
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [HATA] venv olusturulamadi" -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] Yeni venv olusturuldu" -ForegroundColor Green
} else {
    Write-Host "  [OK] Mevcut venv kullaniliyor" -ForegroundColor Green
}

# Kurumsal TLS inspection proxy'leri icin pip'e pypi host'larini guvenilir olarak
# isaretle. Ayrica venv icine pip.ini yaz: sonraki manuel pip kullanimlari da
# (update.cmd dahil) bu ayarlari otomatik alir.
$pipIni = @"
[global]
trusted-host = pypi.org
               files.pythonhosted.org
               pypi.python.org
"@
Set-Content "$venvPath\pip.ini" $pipIni -Encoding ASCII

$pipTrust = @(
    "--trusted-host", "pypi.org",
    "--trusted-host", "files.pythonhosted.org",
    "--trusted-host", "pypi.python.org"
)

& $venvPy -m pip install --upgrade pip --quiet @pipTrust
& $venvPy -m pip install -r "$InstallDir\requirements.txt" --quiet --upgrade @pipTrust
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [HATA] pip install basarisiz" -ForegroundColor Red
    Write-Host "         Asagidaki komutla elle kontrol edin:" -ForegroundColor Yellow
    Write-Host "         $venvPy -m pip install -r $InstallDir\requirements.txt" -ForegroundColor Cyan
    exit 1
}
Write-Host "  [OK] Bagimliliklar kuruldu (duckdb dahil)" -ForegroundColor Green

# pywin32 post-install: COM + servis bilesenleri icin bir kez calistirilir
$pywin32PI = "$venvPath\Scripts\pywin32_postinstall.py"
if (Test-Path $pywin32PI) {
    Write-Host "  pywin32 postinstall calistiriliyor..." -ForegroundColor Gray
    & $venvPy $pywin32PI -install 2>&1 | Out-Null
}

# --- 5. Launcher scriptleri ---
Write-Host "[5/6] Launcher scriptleri..." -ForegroundColor Yellow

$faCmd = @"
@echo off
"$venvPy" "$InstallDir\main.py" --config "$InstallDir\config\config.yaml" %*
"@
Set-Content "$InstallDir\fa.cmd" $faCmd

$dashCmd = @"
@echo off
cd /d "$InstallDir"
echo FILE ACTIVITY Dashboard baslatiliyor (http://localhost:$DashPort)...
"$venvPy" "$InstallDir\main.py" --config "$InstallDir\config\config.yaml" dashboard
pause
"@
Set-Content "$InstallDir\start_dashboard.cmd" $dashCmd

# Update launcher: ayni script'i tekrar cagirir (guncelleme)
# Eski PowerShell'lerde TLS 1.2'yi onceden set etmek zorunlu (irm basarisiz olmasin)
# Issue #77: update'ten ONCE SQLite snapshot al — guncelleme bozulursa
# operator hizlica geri donebilir. Snapshot basarisiz olsa bile update
# devam eder (snapshot olmadan da olabilir, ama update durmamali).
$updateCmd = @"
@echo off
echo FILE ACTIVITY guncelleniyor (master branch)...
echo  - Pre-update SQLite snapshot aliniyor...
cd /d "$InstallDir"
"$InstallDir\.venv\Scripts\python.exe" -m src.storage.backup_manager snapshot --reason "update"
if errorlevel 1 (
    echo  [!] Snapshot basarisiz - update yine de devam ediyor
)
powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; irm https://raw.githubusercontent.com/$RepoOwner/$RepoName/$Branch/deploy/setup-source.ps1 | iex"
"@
Set-Content "$InstallDir\update.cmd" $updateCmd

Write-Host "  [OK] fa.cmd, start_dashboard.cmd, update.cmd" -ForegroundColor Green

# --- 5b. PowerShell module (Import-Module FileActivity) ---
$psModuleSrc  = Join-Path $srcRoot 'powershell\FileActivity'
$psModuleRoot = Join-Path $InstallDir 'powershell'
$psModuleDest = Join-Path $psModuleRoot 'FileActivity'
if (Test-Path $psModuleSrc) {
    if (-not (Test-Path $psModuleRoot)) {
        New-Item -Path $psModuleRoot -ItemType Directory -Force | Out-Null
    }
    if (Test-Path $psModuleDest) { Remove-Item $psModuleDest -Recurse -Force }
    Copy-Item -Path $psModuleSrc -Destination $psModuleDest -Recurse -Force

    # Append to PSModulePath (User scope) if not already present
    $userPSModule = [Environment]::GetEnvironmentVariable('PSModulePath', 'User')
    if (-not $userPSModule) { $userPSModule = '' }
    if ($userPSModule -notlike "*$psModuleRoot*") {
        $newPath = if ($userPSModule) { "$userPSModule;$psModuleRoot" } else { $psModuleRoot }
        [Environment]::SetEnvironmentVariable('PSModulePath', $newPath, 'User')
    }
    Write-Host "  [OK] PowerShell module installed (Import-Module FileActivity)" -ForegroundColor Green
}

# --- 6. Firewall ---
Write-Host "[6/6] Firewall kurali (port $DashPort)..." -ForegroundColor Yellow
try {
    netsh advfirewall firewall delete rule name="FileActivity Dashboard" 2>$null | Out-Null
    netsh advfirewall firewall add rule name="FileActivity Dashboard" dir=in action=allow protocol=tcp localport=$DashPort 2>$null | Out-Null
    Write-Host "  [OK] Firewall kurali eklendi" -ForegroundColor Green
} catch {
    Write-Host "  [UYARI] Firewall kurali eklenemedi" -ForegroundColor Yellow
}

# --- Temizlik ---
Remove-Item $zipPath -Force -ErrorAction SilentlyContinue
Remove-Item $extractPath -Recurse -Force -ErrorAction SilentlyContinue

# --- Ozet ---
Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host "  |  Kurulum Tamamlandi!                     |" -ForegroundColor Green
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host ""
Write-Host "  Kurulum:   $InstallDir"                       -ForegroundColor White
Write-Host "  Dashboard: http://localhost:$DashPort"        -ForegroundColor Yellow
Write-Host ""
Write-Host "  Komutlar:" -ForegroundColor White
Write-Host "    $InstallDir\start_dashboard.cmd   - Dashboard'u baslat" -ForegroundColor Cyan
Write-Host "    $InstallDir\fa.cmd <komut>        - CLI (scan, source, restore ...)" -ForegroundColor Cyan
Write-Host "    $InstallDir\update.cmd            - En son master'a guncelle" -ForegroundColor Cyan
Write-Host ""

# --- Otomatik baslatma ---
$answer = Read-Host "  Dashboard simdi baslasin mi? (E/H) [E]"
if ($answer -ne "H" -and $answer -ne "h") {
    Write-Host "  Dashboard baslatiliyor..." -ForegroundColor Cyan
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "`"$InstallDir\start_dashboard.cmd`"" -WindowStyle Normal
    Start-Sleep -Seconds 3
    Start-Process "http://localhost:$DashPort"
    Write-Host "  [OK] Dashboard baslatildi. Tarayici acilmadiysa: http://localhost:$DashPort" -ForegroundColor Green
}
Write-Host ""
