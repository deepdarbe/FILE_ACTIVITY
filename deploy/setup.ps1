<#
.SYNOPSIS
    FILE ACTIVITY - Tek Komutla Kurulum
.DESCRIPTION
    GitHub'dan ZIP indirir, Python yoksa kurar, dashboard'u baslatir.
    Git gerektirmez.

    Kullanim:
    powershell -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1 | iex"
#>

try {

$ErrorActionPreference = "Continue"
$InstallDir = "C:\FileActivity"
$ZipUrl = "https://github.com/deepdarbe/FILE_ACTIVITY/archive/refs/heads/master.zip"

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host "  |  FILE ACTIVITY - Kurulum                 |" -ForegroundColor Cyan
Write-Host "  |  Windows File Share Analysis System      |" -ForegroundColor Cyan
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host ""

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls

# ─── 1. Python kontrolu ───
Write-Host "  [1/5] Python kontrol ediliyor..." -ForegroundColor Yellow

# PATH yenile + bilinen dizinleri ekle
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
@("C:\Program Files\Python312", "C:\Program Files\Python312\Scripts",
  "C:\Program Files\Python311", "C:\Program Files\Python311\Scripts",
  "C:\Program Files\Python310", "C:\Program Files\Python310\Scripts",
  "C:\Python312", "C:\Python312\Scripts", "C:\Python311", "C:\Python311\Scripts",
  "C:\Python310", "C:\Python310\Scripts") | ForEach-Object {
    if ((Test-Path $_) -and ($env:Path -notlike "*$_*")) { $env:Path += ";$_" }
}

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    Write-Host "  Python bulunamadi. Kuruluyor..." -ForegroundColor Yellow
    $pyInstaller = "$env:TEMP\python_setup.exe"
    $pyUrl = "https://www.python.org/ftp/python/3.12.8/python-3.12.8-amd64.exe"

    # Indir
    Write-Host "    Indiriliyor: $pyUrl" -ForegroundColor Gray
    try {
        $wc = New-Object System.Net.WebClient
        $wc.Headers.Add("User-Agent", "PowerShell")
        $wc.DownloadFile($pyUrl, $pyInstaller)
    } catch {
        try { Invoke-WebRequest -Uri $pyUrl -OutFile $pyInstaller -UseBasicParsing } catch {}
    }

    if (Test-Path $pyInstaller) {
        Write-Host "    Kuruluyor..." -ForegroundColor Gray
        Start-Process -FilePath $pyInstaller -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_pip=1" -Wait
        Remove-Item $pyInstaller -Force -ErrorAction SilentlyContinue
        # PATH yenile
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
    } else {
        Write-Host "  [HATA] Python indirilemedi." -ForegroundColor Red
        Write-Host "  Manuel kurun: https://python.org/downloads" -ForegroundColor Yellow
        Write-Host "  Kurdukten sonra bu komutu tekrar calistirin." -ForegroundColor Yellow
        Read-Host "  Enter'a basin"; exit 1
    }

    $python = Get-Command python -ErrorAction SilentlyContinue
    if (-not $python) {
        Write-Host "  [HATA] Python kuruldu ama bulunamiyor. Terminali kapatip acin ve tekrar calistirin." -ForegroundColor Red
        Read-Host "  Enter'a basin"; exit 1
    }
}
$pyVer = python --version 2>&1
Write-Host "  [OK] $pyVer" -ForegroundColor Green

# ─── 2. Dizin yapisi ───
Write-Host "  [2/5] Dizin yapisi olusturuluyor..." -ForegroundColor Yellow
@($InstallDir, "$InstallDir\config", "$InstallDir\data", "$InstallDir\logs", "$InstallDir\reports") | ForEach-Object {
    New-Item -Path $_ -ItemType Directory -Force -ErrorAction SilentlyContinue | Out-Null
}
Write-Host "  [OK] $InstallDir" -ForegroundColor Green

# ─── 3. Kaynak kodu indir (ZIP - Git gerektirmez) ───
Write-Host "  [3/5] Kaynak kod indiriliyor..." -ForegroundColor Yellow
$zipPath = "$env:TEMP\FileActivity.zip"
$extractPath = "$env:TEMP\FileActivity_extract"

# Indir
Write-Host "    GitHub'dan indiriliyor..." -ForegroundColor Gray
try {
    $wc = New-Object System.Net.WebClient
    $wc.Headers.Add("User-Agent", "PowerShell")
    $wc.DownloadFile($ZipUrl, $zipPath)
} catch {
    try { Invoke-WebRequest -Uri $ZipUrl -OutFile $zipPath -UseBasicParsing } catch {}
}

if (-not (Test-Path $zipPath)) {
    Write-Host "  [HATA] Kaynak kod indirilemedi!" -ForegroundColor Red
    Read-Host "  Enter'a basin"; exit 1
}
$zipSize = [math]::Round((Get-Item $zipPath).Length / 1MB, 1)
Write-Host "    Indirildi: $zipSize MB" -ForegroundColor Gray

# Ac
Write-Host "    ZIP aciliyor..." -ForegroundColor Gray
if (Test-Path $extractPath) { Remove-Item $extractPath -Recurse -Force }
Expand-Archive -Path $zipPath -DestinationPath $extractPath -Force

# GitHub ZIP icindeki klasor adini bul (FILE_ACTIVITY-master gibi)
$innerDir = Get-ChildItem $extractPath -Directory | Select-Object -First 1

# Repo dizinine kopyala
$repoDir = "$InstallDir\repo"
if (Test-Path $repoDir) { Remove-Item $repoDir -Recurse -Force }
Move-Item $innerDir.FullName $repoDir

# Temizle
Remove-Item $zipPath -Force -ErrorAction SilentlyContinue
Remove-Item $extractPath -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "  [OK] Kaynak kod hazir" -ForegroundColor Green

# ─── 4. Bagimliliklar ───
Write-Host "  [4/5] Python bagimliliklari kuruluyor..." -ForegroundColor Yellow
python -m pip install -r "$repoDir\requirements.txt" --quiet 2>$null
Write-Host "  [OK] Bagimliliklar kuruldu" -ForegroundColor Green

# ─── 5. Config ───
Write-Host "  [5/5] Konfigurasyon ayarlaniyor..." -ForegroundColor Yellow
$configDest = "$InstallDir\config\config.yaml"
if (-not (Test-Path $configDest)) {
    Copy-Item "$repoDir\config.yaml" $configDest
    $content = Get-Content $configDest -Raw
    $dbPath = "$InstallDir\data\file_activity.db" -replace '\\', '\\'
    $logPath = "$InstallDir\logs/" -replace '\\', '\\'
    $content = $content -replace 'path: "data/file_activity.db"', "path: `"$dbPath`""
    Set-Content $configDest $content
    Write-Host "  [OK] config.yaml olusturuldu" -ForegroundColor Green
} else {
    Write-Host "  [OK] config.yaml mevcut (korundu)" -ForegroundColor Green
}

# Firewall
try {
    New-NetFirewallRule -DisplayName "FileActivity Dashboard" `
        -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8085 `
        -ErrorAction SilentlyContinue | Out-Null
} catch {}

# ─── Tamamlandi ───
Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host "  |  Kurulum Tamamlandi!                     |" -ForegroundColor Green
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host ""
Write-Host "  Kurulum: $InstallDir" -ForegroundColor White
Write-Host "  Dashboard: http://localhost:8085" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Manuel baslatma:" -ForegroundColor White
Write-Host "    python $repoDir\main.py dashboard --config $configDest" -ForegroundColor Cyan
Write-Host ""

# Dashboard'u baslat mi?
$start = Read-Host "  Dashboard simdi baslatilsin mi? (E/H)"
if ($start -eq "E" -or $start -eq "e" -or $start -eq "") {
    Write-Host "  Dashboard baslatiliyor..." -ForegroundColor Cyan
    Start-Process -FilePath "python" -ArgumentList "$repoDir\main.py", "dashboard", "--config", $configDest
    Start-Sleep -Seconds 3
    Start-Process "http://localhost:8085"
    Write-Host "  [OK] Dashboard baslatildi!" -ForegroundColor Green
}

} catch {
    Write-Host ""
    Write-Host "  [HATA] $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
}

Write-Host ""
Write-Host "  Kapatmak icin bir tusa basin..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
