<#
.SYNOPSIS
    Install FILE ACTIVITY as a Windows Service via NSSM (Non-Sucking Service Manager).

.DESCRIPTION
    Issue #151: customers complain that closing/opening the dashboard via cmd
    prompts and Task Manager kills is tedious. This script wraps the dashboard
    entry point as a real Windows service so it:

      - Starts automatically on boot (SERVICE_AUTO_START)
      - Restarts on crash (AppExit Default Restart)
      - Logs stdout/stderr to logs\service.out / logs\service.err with rotation
      - Can be controlled with Start-Service / Stop-Service / Get-Service

    NSSM is downloaded on demand from https://nssm.cc/release/nssm-2.24.zip
    (BSD-like license, free for commercial use). The 64-bit binary is placed
    under <InstallDir>\bin\nssm.exe.

    Existing start_dashboard.cmd keeps working for debug / standalone use.
    Customers who do not run this script see zero behavior change (issue #151
    backwards compat constraint).

.PARAMETER InstallDir
    FILE ACTIVITY install root (default C:\FileActivity).

.PARAMETER ServiceName
    Windows service name (default FileActivity).

.PARAMETER NssmUrl
    NSSM zip download URL. Override only if your network blocks nssm.cc.

.PARAMETER InstallTray
    Switch — also drop the optional system tray helper into shell:startup
    (auto-starts at user logon). Off by default.

.EXAMPLE
    # Run from elevated PowerShell:
    powershell -ExecutionPolicy Bypass -File C:\FileActivity\deploy\install_service.ps1

.EXAMPLE
    # Service + tray auto-start in one shot:
    powershell -ExecutionPolicy Bypass -File C:\FileActivity\deploy\install_service.ps1 -InstallTray

.NOTES
    Requires Administrator. Companion: deploy\uninstall_service.ps1.
#>

[CmdletBinding()]
param(
    [string]$InstallDir = "C:\FileActivity",
    [string]$ServiceName = "FileActivity",
    [string]$NssmUrl = "https://nssm.cc/release/nssm-2.24.zip",
    [switch]$InstallTray
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host "  |  FILE ACTIVITY - Service Kurulumu (NSSM) |" -ForegroundColor Cyan
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host ""

# --- Yonetici kontrolu ---
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "  [HATA] Bu script Yonetici olarak calistirilmalidir." -ForegroundColor Red
    Write-Host "  PowerShell'i sag tikla -> 'Yonetici olarak calistir', sonra tekrar deneyin." -ForegroundColor Yellow
    exit 1
}

# --- Install root sanity ---
$venvPy = Join-Path $InstallDir ".venv\Scripts\python.exe"
$mainPy = Join-Path $InstallDir "main.py"
$cfgYaml = Join-Path $InstallDir "config\config.yaml"

if (-not (Test-Path $venvPy)) {
    Write-Host "  [HATA] $venvPy bulunamadi." -ForegroundColor Red
    Write-Host "         Once setup-source.ps1 ile kurulum yapin." -ForegroundColor Yellow
    exit 1
}
if (-not (Test-Path $mainPy)) {
    Write-Host "  [HATA] $mainPy bulunamadi." -ForegroundColor Red
    exit 1
}
if (-not (Test-Path $cfgYaml)) {
    Write-Host "  [UYARI] $cfgYaml bulunamadi - servis baslatildiginda config hatasi alabilirsiniz." -ForegroundColor Yellow
}

$binDir = Join-Path $InstallDir "bin"
$logsDir = Join-Path $InstallDir "logs"
foreach ($d in @($binDir, $logsDir)) {
    if (-not (Test-Path $d)) { New-Item -Path $d -ItemType Directory -Force | Out-Null }
}

# --- 1. nssm.exe hazirla ---
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$nssmExe = Join-Path $binDir "nssm.exe"

if (-not (Test-Path $nssmExe)) {
    Write-Host "[1/4] NSSM indiriliyor ($NssmUrl)..." -ForegroundColor Yellow
    $tmpZip = Join-Path $env:TEMP "nssm-install.zip"
    $tmpExtract = Join-Path $env:TEMP "nssm-extract"
    if (Test-Path $tmpZip)     { Remove-Item $tmpZip -Force }
    if (Test-Path $tmpExtract) { Remove-Item $tmpExtract -Recurse -Force }

    try {
        Invoke-WebRequest -Uri $NssmUrl -OutFile $tmpZip -UseBasicParsing
    } catch {
        Write-Host "  [HATA] NSSM indirilemedi: $_" -ForegroundColor Red
        Write-Host "         Manuel: $NssmUrl indirip $nssmExe konumuna kopyalayin." -ForegroundColor Yellow
        exit 1
    }

    Expand-Archive -Path $tmpZip -DestinationPath $tmpExtract -Force

    # NSSM zip ships win32/ + win64/. Prefer 64-bit on 64-bit OS.
    $arch = if ([Environment]::Is64BitOperatingSystem) { "win64" } else { "win32" }
    $extractedNssm = Get-ChildItem -Path $tmpExtract -Recurse -Filter "nssm.exe" |
        Where-Object { $_.FullName -match "\\$arch\\" } |
        Select-Object -First 1
    if (-not $extractedNssm) {
        # Fallback: any nssm.exe in archive
        $extractedNssm = Get-ChildItem -Path $tmpExtract -Recurse -Filter "nssm.exe" | Select-Object -First 1
    }
    if (-not $extractedNssm) {
        Write-Host "  [HATA] nssm.exe arsiv icinde bulunamadi." -ForegroundColor Red
        exit 1
    }
    Copy-Item -Path $extractedNssm.FullName -Destination $nssmExe -Force
    Remove-Item $tmpZip -Force -ErrorAction SilentlyContinue
    Remove-Item $tmpExtract -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "  [OK] nssm.exe -> $nssmExe" -ForegroundColor Green
} else {
    Write-Host "[1/4] NSSM zaten mevcut: $nssmExe" -ForegroundColor Green
}

# --- 2. Existing service varsa once kaldir ---
$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "[2/4] Mevcut servis bulundu, yeniden olusturmak icin kaldiriliyor..." -ForegroundColor Yellow
    & $nssmExe stop $ServiceName 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    & $nssmExe remove $ServiceName confirm 2>&1 | Out-Null
    Start-Sleep -Seconds 1
    Write-Host "  [OK] Eski servis kaldirildi" -ForegroundColor Green
} else {
    Write-Host "[2/4] Yeni servis olusturuluyor: $ServiceName" -ForegroundColor Yellow
}

# --- 3. NSSM ile servisi konfigure et ---
Write-Host "[3/4] NSSM servis konfigurasyonu..." -ForegroundColor Yellow

$svcOut = Join-Path $logsDir "service.out"
$svcErr = Join-Path $logsDir "service.err"

# nssm install <name> <bin> <args...>
& $nssmExe install $ServiceName $venvPy $mainPy "--config" $cfgYaml "dashboard"
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [HATA] nssm install basarisiz (exit $LASTEXITCODE)" -ForegroundColor Red
    exit 1
}

# Working directory so relative paths in main.py resolve correctly.
& $nssmExe set $ServiceName AppDirectory $InstallDir | Out-Null

# Log redirection + rotation
& $nssmExe set $ServiceName AppStdout $svcOut | Out-Null
& $nssmExe set $ServiceName AppStderr $svcErr | Out-Null
& $nssmExe set $ServiceName AppRotateFiles 1 | Out-Null
& $nssmExe set $ServiceName AppRotateOnline 1 | Out-Null
& $nssmExe set $ServiceName AppRotateBytes 10485760 | Out-Null  # 10 MB

# Start type: auto on boot
& $nssmExe set $ServiceName Start SERVICE_AUTO_START | Out-Null

# Crash recovery: restart on any unexpected exit, throttle 5s
& $nssmExe set $ServiceName AppExit Default Restart | Out-Null
& $nssmExe set $ServiceName AppRestartDelay 5000 | Out-Null
& $nssmExe set $ServiceName AppThrottle 10000 | Out-Null

# Display name + description
& $nssmExe set $ServiceName DisplayName "FILE ACTIVITY - File Share Monitor" | Out-Null
& $nssmExe set $ServiceName Description "Windows File Share Analysis, Monitoring and Archiving Service. Issue #151: NSSM-managed dashboard with auto-start + crash recovery." | Out-Null

Write-Host "  [OK] Servis konfigure edildi" -ForegroundColor Green
Write-Host "       stdout : $svcOut" -ForegroundColor DarkGray
Write-Host "       stderr : $svcErr" -ForegroundColor DarkGray
Write-Host "       restart: 5 sn delay, otomatik" -ForegroundColor DarkGray

# --- 4. Servisi baslat ---
# NSSM'in stderr'e UTF-16 yazdigi "SERVICE_START_PENDING" satiri Windows SCM'in
# normal cevabidir (asenkron baslatma). PowerShell'in NativeCommandError record'u
# ile karistirmamak icin Start-Process ile cagiriyor, stdout/stderr'i tamamen yutuyoruz.
# Sonra Get-Service ile gercek durumu polluyoruz (15 sn'ye kadar) — ilk boot venv +
# APScheduler nedeniyle 8-15 sn alabilir.
Write-Host "[4/4] Servis baslatiliyor..." -ForegroundColor Yellow
$null = Start-Process -FilePath $nssmExe -ArgumentList "start", $ServiceName `
    -Wait -NoNewWindow `
    -RedirectStandardOutput "$env:TEMP\nssm_start.out" `
    -RedirectStandardError "$env:TEMP\nssm_start.err"
Remove-Item "$env:TEMP\nssm_start.out", "$env:TEMP\nssm_start.err" -ErrorAction SilentlyContinue

$svc = $null
for ($i = 0; $i -lt 15; $i++) {
    Start-Sleep -Seconds 1
    $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($svc -and $svc.Status -eq "Running") { break }
}
if ($svc -and $svc.Status -eq "Running") {
    Write-Host "  [OK] Servis calisiyor (Status=Running, $($i+1) sn icinde)" -ForegroundColor Green
} else {
    $statusText = if ($svc) { $svc.Status } else { "<servis bulunamadi>" }
    Write-Host "  [UYARI] Servis 15 sn icinde Running'e gecmedi. Status: $statusText" -ForegroundColor Yellow
    Write-Host "          Son 20 satir log: $svcErr" -ForegroundColor Yellow
    if (Test-Path $svcErr) {
        Get-Content $svcErr -Tail 20 | ForEach-Object {
            Write-Host "            $_" -ForegroundColor DarkGray
        }
    }
}

# --- Optional tray app auto-start ---
if ($InstallTray) {
    Write-Host ""
    Write-Host "[+] Tray app auto-start kuruluyor..." -ForegroundColor Yellow
    $trayInstaller = Join-Path $InstallDir "deploy\install_tray.ps1"
    if (Test-Path $trayInstaller) {
        & powershell -ExecutionPolicy Bypass -File $trayInstaller -InstallDir $InstallDir
    } else {
        Write-Host "  [UYARI] $trayInstaller bulunamadi - tray app atlandi." -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host "  |  Servis Kurulumu Tamam!                  |" -ForegroundColor Green
Write-Host "  +==========================================+" -ForegroundColor Green
Write-Host ""
Write-Host "  Servis adi : $ServiceName" -ForegroundColor White
Write-Host "  Dashboard  : http://localhost:8085" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Yonetim:" -ForegroundColor White
Write-Host "    Start-Service $ServiceName       # baslat" -ForegroundColor Cyan
Write-Host "    Stop-Service $ServiceName        # durdur" -ForegroundColor Cyan
Write-Host "    Restart-Service $ServiceName     # yeniden baslat" -ForegroundColor Cyan
Write-Host "    Get-Service $ServiceName         # durum" -ForegroundColor Cyan
Write-Host ""
Write-Host "  PowerShell module cmdlet'leri (Import-Module FileActivity):" -ForegroundColor White
Write-Host "    Start-FileActivityService" -ForegroundColor Cyan
Write-Host "    Stop-FileActivityService" -ForegroundColor Cyan
Write-Host "    Restart-FileActivityService" -ForegroundColor Cyan
Write-Host "    Get-FileActivityServiceStatus" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Kaldirma: $InstallDir\deploy\uninstall_service.ps1" -ForegroundColor White
Write-Host ""
