<#
.SYNOPSIS
    Install the FILE ACTIVITY system tray helper (issue #151) and add it to
    the current user's startup folder so it auto-launches at logon.

.DESCRIPTION
    1. pip install -r requirements-tray.txt   (pystray + Pillow into venv)
    2. Drop a launcher batch at <InstallDir>\start_tray.cmd
    3. Create a Startup folder shortcut so the tray icon comes up at logon
       (per-user, no Admin needed for the shortcut step itself)

    The tray app talks to the FileActivity Windows service via PowerShell;
    install_service.ps1 should already have been run.

.PARAMETER InstallDir
    FILE ACTIVITY install root (default C:\FileActivity).

.PARAMETER ServiceName
    Windows service name controlled by the tray app (default FileActivity).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File C:\FileActivity\deploy\install_tray.ps1

.NOTES
    Tray is opt-in. To remove: delete <Startup>\FileActivityTray.lnk and
    optionally pip uninstall pystray Pillow inside the venv.
#>

[CmdletBinding()]
param(
    [string]$InstallDir = "C:\FileActivity",
    [string]$ServiceName = "FileActivity"
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host "  |  FILE ACTIVITY - Tray App Kurulumu       |" -ForegroundColor Cyan
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host ""

$venvPy = Join-Path $InstallDir ".venv\Scripts\python.exe"
$venvPyW = Join-Path $InstallDir ".venv\Scripts\pythonw.exe"
$reqTray = Join-Path $InstallDir "requirements-tray.txt"

if (-not (Test-Path $venvPy)) {
    Write-Host "  [HATA] $venvPy yok. Once setup-source.ps1 calistirin." -ForegroundColor Red
    exit 1
}
if (-not (Test-Path $reqTray)) {
    Write-Host "  [HATA] $reqTray yok - tray app dosyalari kurulumda eksik." -ForegroundColor Red
    exit 1
}

Write-Host "[1/3] pystray + Pillow yukleniyor..." -ForegroundColor Yellow
$pipTrust = @(
    "--trusted-host", "pypi.org",
    "--trusted-host", "files.pythonhosted.org",
    "--trusted-host", "pypi.python.org"
)
& $venvPy -m pip install -r $reqTray --quiet @pipTrust
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [HATA] pip install basarisiz" -ForegroundColor Red
    exit 1
}
Write-Host "  [OK] Bagimliliklar kuruldu" -ForegroundColor Green

Write-Host "[2/3] start_tray.cmd olusturuluyor..." -ForegroundColor Yellow
$trayLauncher = Join-Path $InstallDir "start_tray.cmd"
# pythonw.exe -> no console window; fallback to python.exe if absent
$pyExe = if (Test-Path $venvPyW) { $venvPyW } else { $venvPy }
$trayCmd = @"
@echo off
cd /d "$InstallDir"
start "" "$pyExe" -m src.tray.tray_app --service-name $ServiceName --install-dir "$InstallDir"
"@
Set-Content -Path $trayLauncher -Value $trayCmd -Encoding ASCII
Write-Host "  [OK] $trayLauncher" -ForegroundColor Green

Write-Host "[3/3] Startup klasorune kisayol ekleniyor..." -ForegroundColor Yellow
$startupDir = [Environment]::GetFolderPath("Startup")
$shortcutPath = Join-Path $startupDir "FileActivityTray.lnk"
try {
    $wsh = New-Object -ComObject WScript.Shell
    $sc = $wsh.CreateShortcut($shortcutPath)
    $sc.TargetPath = $trayLauncher
    $sc.WorkingDirectory = $InstallDir
    $sc.WindowStyle = 7  # minimized
    $sc.Description = "FILE ACTIVITY tray icon (issue #151)"
    $sc.Save()
    Write-Host "  [OK] $shortcutPath" -ForegroundColor Green
} catch {
    Write-Host "  [UYARI] Shortcut olusturulamadi: $_" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  Tray app kuruldu. Simdi baslatmak icin:" -ForegroundColor White
Write-Host "    $trayLauncher" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Bir sonraki logon'da otomatik baslar (Startup klasor kisayolu)." -ForegroundColor White
Write-Host ""
