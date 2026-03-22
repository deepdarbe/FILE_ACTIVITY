<#
.SYNOPSIS
    FILE ACTIVITY - Tek Komutla Kurulum
.DESCRIPTION
    GitHub'dan klonlar, bagimliliklari kurar, dizin yapisini olusturur ve dashboard'u baslatir.
    Kullanim: irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1 | iex
#>

$ErrorActionPreference = "Stop"
$InstallDir = "C:\FileActivity"
$RepoUrl = "https://github.com/deepdarbe/FILE_ACTIVITY.git"
$Branch = "master"

Write-Host ""
Write-Host "  ╔══════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║  FILE ACTIVITY - Kurulum                 ║" -ForegroundColor Cyan
Write-Host "  ║  Windows File Share Analysis System      ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ─── 1. Git kontrolu ───
Write-Host "  [1/6] Git kontrol ediliyor..." -ForegroundColor Yellow
$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
    Write-Host "  Git bulunamadi. Kuruluyor..." -ForegroundColor Gray
    try {
        winget install --id Git.Git --accept-package-agreements --accept-source-agreements --silent 2>$null
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
        $git = Get-Command git -ErrorAction SilentlyContinue
        if (-not $git) {
            Write-Host "  [!] Git kuruldu ama PATH'e eklenmesi icin terminali yeniden acin." -ForegroundColor Red
            Write-Host "  Sonra bu komutu tekrar calistirin." -ForegroundColor Red
            exit 1
        }
    } catch {
        Write-Host "  [HATA] Git kurulamadi. Manuel kurun: https://git-scm.com" -ForegroundColor Red
        exit 1
    }
}
Write-Host "  [OK] Git: $(git --version)" -ForegroundColor Green

# ─── 2. Python kontrolu ───
Write-Host "  [2/6] Python kontrol ediliyor..." -ForegroundColor Yellow
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    Write-Host "  Python bulunamadi. Kuruluyor..." -ForegroundColor Gray
    try {
        winget install --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements --silent 2>$null
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
        $python = Get-Command python -ErrorAction SilentlyContinue
        if (-not $python) {
            Write-Host "  [!] Python kuruldu ama PATH'e eklenmesi icin terminali yeniden acin." -ForegroundColor Red
            exit 1
        }
    } catch {
        Write-Host "  [HATA] Python kurulamadi. Manuel kurun: https://python.org" -ForegroundColor Red
        exit 1
    }
}
$pyVer = python --version 2>&1
Write-Host "  [OK] $pyVer" -ForegroundColor Green

# ─── 3. Dizin yapisi ───
Write-Host "  [3/6] Dizin yapisi olusturuluyor..." -ForegroundColor Yellow
$dirs = @("config", "data", "logs", "reports")
New-Item -Path $InstallDir -ItemType Directory -Force -ErrorAction SilentlyContinue | Out-Null
foreach ($d in $dirs) {
    New-Item -Path "$InstallDir\$d" -ItemType Directory -Force -ErrorAction SilentlyContinue | Out-Null
}
Write-Host "  [OK] $InstallDir" -ForegroundColor Green

# ─── 4. Repo klonla veya guncelle ───
Write-Host "  [4/6] Kaynak kod indiriliyor..." -ForegroundColor Yellow
$repoDir = "$InstallDir\repo"
if (Test-Path "$repoDir\.git") {
    Write-Host "  Mevcut repo guncelleniyor..." -ForegroundColor Gray
    Set-Location $repoDir
    git pull origin $Branch --quiet 2>$null
} else {
    if (Test-Path $repoDir) { Remove-Item $repoDir -Recurse -Force }
    git clone --branch $Branch --single-branch $RepoUrl $repoDir --quiet 2>$null
}
Write-Host "  [OK] Kaynak kod hazir" -ForegroundColor Green

# ─── 5. Bagimliliklar ───
Write-Host "  [5/6] Python bagimliliklari kuruluyor..." -ForegroundColor Yellow
pip install -r "$repoDir\requirements.txt" --quiet 2>$null
Write-Host "  [OK] Bagimliliklar kuruldu" -ForegroundColor Green

# ─── 6. Config ───
Write-Host "  [6/6] Konfigürasyon ayarlaniyor..." -ForegroundColor Yellow
$configDest = "$InstallDir\config\config.yaml"
if (-not (Test-Path $configDest)) {
    Copy-Item "$repoDir\config.yaml" $configDest
    # Database yolunu guncelle
    $content = Get-Content $configDest -Raw
    $content = $content -replace 'path: "data/file_activity.db"', "path: `"$InstallDir\data\file_activity.db`""
    $content = $content -replace 'log_file: "logs/', "log_file: `"$InstallDir\logs/"
    Set-Content $configDest $content
    Write-Host "  [OK] config.yaml olusturuldu" -ForegroundColor Green
} else {
    Write-Host "  [OK] config.yaml mevcut (korundu)" -ForegroundColor Green
}

# ─── Firewall ───
try {
    New-NetFirewallRule -DisplayName "FileActivity Dashboard" `
        -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8085 `
        -ErrorAction SilentlyContinue | Out-Null
} catch {}

# ─── Baslat ───
Write-Host ""
Write-Host "  ╔══════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "  ║  Kurulum Tamamlandi!                     ║" -ForegroundColor Green
Write-Host "  ╚══════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "  Dashboard baslatmak icin:" -ForegroundColor White
Write-Host "    python $repoDir\main.py dashboard --config $configDest" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Otomatik guncelleme zamanlama:" -ForegroundColor White
Write-Host "    powershell -File $repoDir\deploy\auto-update.ps1 -SetupSchedule" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Dashboard: http://localhost:8085" -ForegroundColor Yellow
Write-Host ""

# Dashboard'u baslat mi?
$start = Read-Host "  Dashboard simdi baslatilsin mi? (E/H)"
if ($start -eq "E" -or $start -eq "e") {
    Write-Host "  Dashboard baslatiliyor..." -ForegroundColor Cyan
    Start-Process -FilePath "python" -ArgumentList "$repoDir\main.py", "dashboard", "--config", $configDest
    Start-Sleep -Seconds 3
    Start-Process "http://localhost:8085"
}
