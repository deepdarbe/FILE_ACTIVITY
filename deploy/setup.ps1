<#
.SYNOPSIS
    FILE ACTIVITY - Tek Komutla Kurulum (EXE - Python gerektirmez)
.DESCRIPTION
    GitHub Releases'tan EXE paketini indirir, kurar ve dashboard'u baslatir.
    Hedef sunucuda Python veya Git kurulu olmasi GEREKMEZ.

    Kullanim:
    powershell -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1 | iex"
#>

try {

$ErrorActionPreference = "Continue"
$InstallDir = "C:\FileActivity"
$ReleaseApi = "https://api.github.com/repos/deepdarbe/FILE_ACTIVITY/releases/latest"

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host "  |  FILE ACTIVITY - Kurulum                 |" -ForegroundColor Cyan
Write-Host "  |  Standalone EXE - Python gerektirmez     |" -ForegroundColor Cyan
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host ""

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls

# ─── 1. Son surumu bul ───
Write-Host "  [1/4] Son surum kontrol ediliyor..." -ForegroundColor Yellow
try {
    $release = Invoke-RestMethod -Uri $ReleaseApi -Headers @{"User-Agent"="PowerShell"} -UseBasicParsing
    $version = $release.tag_name
    $asset = $release.assets | Where-Object { $_.name -like "*.zip" } | Select-Object -First 1
    $downloadUrl = $asset.browser_download_url
    $fileName = $asset.name
    $fileSize = [math]::Round($asset.size / 1MB, 1)
    Write-Host "  [OK] Surum: $version ($fileSize MB)" -ForegroundColor Green
} catch {
    Write-Host "  GitHub API hatasi. Dogrudan URL deneniyor..." -ForegroundColor Yellow
    $downloadUrl = "https://github.com/deepdarbe/FILE_ACTIVITY/releases/latest/download/FileActivity-Deploy.zip"
    $fileName = "FileActivity-Deploy.zip"
    $version = "latest"
}

# ─── 2. Indir ───
Write-Host "  [2/4] EXE paketi indiriliyor..." -ForegroundColor Yellow
$zipPath = "$env:TEMP\$fileName"

Write-Host "    Indiriliyor: $downloadUrl" -ForegroundColor Gray
try {
    $wc = New-Object System.Net.WebClient
    $wc.Headers.Add("User-Agent", "PowerShell")
    $wc.DownloadFile($downloadUrl, $zipPath)
} catch {
    try {
        Invoke-WebRequest -Uri $downloadUrl -OutFile $zipPath -UseBasicParsing
    } catch {
        Write-Host "  [HATA] Paket indirilemedi: $_" -ForegroundColor Red
        Read-Host "  Enter'a basin"; exit 1
    }
}

if (-not (Test-Path $zipPath)) {
    Write-Host "  [HATA] Dosya indirilemedi!" -ForegroundColor Red
    Read-Host "  Enter'a basin"; exit 1
}
$dlSize = [math]::Round((Get-Item $zipPath).Length / 1MB, 1)
Write-Host "  [OK] Indirildi: $dlSize MB" -ForegroundColor Green

# ─── 3. Kur ───
Write-Host "  [3/4] Kuruluyor..." -ForegroundColor Yellow

# Mevcut kurulum varsa veriyi koru
$hasExisting = Test-Path "$InstallDir\bin"
if ($hasExisting) {
    Write-Host "    Mevcut kurulum tespit edildi - guncelleme modu" -ForegroundColor Gray
    Write-Host "    Veritabani, config, log korunuyor..." -ForegroundColor Gray
    # Eski bin yedekle
    if (Test-Path "$InstallDir\bin_old") { Remove-Item "$InstallDir\bin_old" -Recurse -Force }
    Rename-Item -Path "$InstallDir\bin" -NewName "bin_old" -ErrorAction SilentlyContinue
}

# Dizin yapisi
@($InstallDir, "$InstallDir\bin", "$InstallDir\config", "$InstallDir\data", "$InstallDir\logs", "$InstallDir\reports", "$InstallDir\scripts") | ForEach-Object {
    New-Item -Path $_ -ItemType Directory -Force -ErrorAction SilentlyContinue | Out-Null
}

# ZIP ac
$extractPath = "$env:TEMP\FA_extract"
if (Test-Path $extractPath) { Remove-Item $extractPath -Recurse -Force }
Write-Host "    ZIP aciliyor..." -ForegroundColor Gray
Expand-Archive -Path $zipPath -DestinationPath $extractPath -Force

# ZIP icerigini bul (FileActivity-Deploy/ klasoru icinde)
$sourceDir = $extractPath
$innerDirs = Get-ChildItem $extractPath -Directory -ErrorAction SilentlyContinue
if ($innerDirs) {
    $sourceDir = $innerDirs[0].FullName
}
Write-Host "    Kaynak: $sourceDir" -ForegroundColor Gray

# bin/ klasorunu kopyala
if (Test-Path "$sourceDir\bin") {
    Write-Host "    bin\ kopyalaniyor..." -ForegroundColor Gray
    Copy-Item -Path "$sourceDir\bin\*" -Destination "$InstallDir\bin\" -Recurse -Force
} else {
    Write-Host "    [!] bin\ klasoru bulunamadi, tum icerik kopyalaniyor..." -ForegroundColor Yellow
    Copy-Item -Path "$sourceDir\*" -Destination "$InstallDir\bin\" -Recurse -Force -ErrorAction SilentlyContinue
}

# Config (sadece yoksa kopyala - mevcut config korunur)
if (-not (Test-Path "$InstallDir\config\config.yaml")) {
    if (Test-Path "$sourceDir\config\config.yaml") {
        Copy-Item -Path "$sourceDir\config\config.yaml" -Destination "$InstallDir\config\" -Force
    } elseif (Test-Path "$sourceDir\config.yaml") {
        Copy-Item -Path "$sourceDir\config.yaml" -Destination "$InstallDir\config\" -Force
    }
    # Yollari guncelle
    if (Test-Path "$InstallDir\config\config.yaml") {
        $content = Get-Content "$InstallDir\config\config.yaml" -Raw
        $content = $content -replace 'path: "data/file_activity.db"', "path: `"$InstallDir\data\file_activity.db`""
        Set-Content "$InstallDir\config\config.yaml" $content
    }
}

# Scripts
if (Test-Path "$sourceDir\scripts") {
    Copy-Item -Path "$sourceDir\scripts\*" -Destination "$InstallDir\scripts\" -Recurse -Force -ErrorAction SilentlyContinue
}

# Temizle
Remove-Item $zipPath -Force -ErrorAction SilentlyContinue
Remove-Item $extractPath -Recurse -Force -ErrorAction SilentlyContinue

# Dogrulama
if (Test-Path "$InstallDir\bin\FileActivity.exe") {
    Write-Host "  [OK] Kurulum tamamlandi: $InstallDir" -ForegroundColor Green
} else {
    Write-Host "  [!] FileActivity.exe bulunamadi. Dizin icerigini kontrol edin:" -ForegroundColor Yellow
    Get-ChildItem "$InstallDir\bin" -ErrorAction SilentlyContinue | Select-Object -First 10 | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
}

# ─── 4. Firewall + Baslat ───
Write-Host "  [4/4] Yapilandiriliyor..." -ForegroundColor Yellow

# Firewall
try {
    New-NetFirewallRule -DisplayName "FileActivity Dashboard" `
        -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8085 `
        -ErrorAction SilentlyContinue | Out-Null
    Write-Host "    Firewall kurali eklendi" -ForegroundColor Gray
} catch {}

# Baslat scripti olustur
$launcher = "$InstallDir\start_dashboard.cmd"
if (Test-Path "$InstallDir\bin\FileActivity.exe") {
    Set-Content $launcher "@echo off`r`ncd /d `"$InstallDir`"`r`n`"$InstallDir\bin\FileActivity.exe`" dashboard --config `"$InstallDir\config\config.yaml`"`r`npause"
    Write-Host "    Launcher olusturuldu: $launcher" -ForegroundColor Gray
}

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host "  |  Kurulum Tamamlandi! ($version)          |" -ForegroundColor Green
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host ""
Write-Host "  Kurulum: $InstallDir" -ForegroundColor White
Write-Host "  Dashboard: http://localhost:8085" -ForegroundColor Yellow
Write-Host ""
if ($hasExisting) {
    Write-Host "  GUNCELLEME - Korunan veriler:" -ForegroundColor Yellow
    Write-Host "    - data\file_activity.db (veritabani)" -ForegroundColor Gray
    Write-Host "    - config\config.yaml (ayarlar)" -ForegroundColor Gray
    Write-Host "    - logs\ ve reports\" -ForegroundColor Gray
    Write-Host "    - Eski surum: bin_old\ (rollback icin)" -ForegroundColor Gray
    Write-Host ""
}

# Dashboard baslat
$start = Read-Host "  Dashboard baslatilsin mi? (E/H)"
if ($start -eq "E" -or $start -eq "e" -or $start -eq "") {
    if (Test-Path "$InstallDir\bin\FileActivity.exe") {
        Write-Host "  Dashboard baslatiliyor..." -ForegroundColor Cyan
        Start-Process -FilePath "$InstallDir\bin\FileActivity.exe" `
            -ArgumentList "dashboard", "--config", "$InstallDir\config\config.yaml" `
            -WorkingDirectory $InstallDir
        Start-Sleep -Seconds 3
        Start-Process "http://localhost:8085"
        Write-Host "  [OK] Dashboard baslatildi!" -ForegroundColor Green
    } else {
        Write-Host "  [!] FileActivity.exe bulunamadi: $InstallDir\bin\" -ForegroundColor Red
        Write-Host "  Dosyalari kontrol edin." -ForegroundColor Yellow
    }
}

} catch {
    Write-Host ""
    Write-Host "  [HATA] $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
}

Write-Host ""
Write-Host "  Kapatmak icin bir tusa basin..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
