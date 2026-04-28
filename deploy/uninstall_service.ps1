<#
.SYNOPSIS
    Remove the FILE ACTIVITY Windows service installed by install_service.ps1.

.DESCRIPTION
    Issue #151 rollback path: stop the NSSM-managed service and remove its
    Windows service entry. NSSM binary itself + log files are left in place
    (under <InstallDir>\bin and <InstallDir>\logs) so the operator can
    inspect them; delete those manually if you want a clean wipe.

    After uninstall, start_dashboard.cmd is the only way to launch the
    dashboard — back to the pre-#151 manual flow.

.PARAMETER InstallDir
    FILE ACTIVITY install root (default C:\FileActivity).

.PARAMETER ServiceName
    Windows service name (default FileActivity).

.PARAMETER RemoveTrayShortcut
    Switch — also delete the tray app shortcut from shell:startup. Off by
    default to avoid surprises (the operator may want to keep it).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File C:\FileActivity\deploy\uninstall_service.ps1

.NOTES
    Requires Administrator. Companion: deploy\install_service.ps1.
#>

[CmdletBinding()]
param(
    [string]$InstallDir = "C:\FileActivity",
    [string]$ServiceName = "FileActivity",
    [switch]$RemoveTrayShortcut
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host "  |  FILE ACTIVITY - Service Kaldirma        |" -ForegroundColor Cyan
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host ""

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "  [HATA] Bu script Yonetici olarak calistirilmalidir." -ForegroundColor Red
    exit 1
}

$nssmExe = Join-Path $InstallDir "bin\nssm.exe"
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue

if (-not $svc) {
    Write-Host "  [BILGI] $ServiceName servisi zaten yok - islem yok." -ForegroundColor Yellow
} else {
    if (Test-Path $nssmExe) {
        Write-Host "  Servis durduruluyor..." -ForegroundColor Yellow
        & $nssmExe stop $ServiceName 2>&1 | Out-Null
        Start-Sleep -Seconds 2
        Write-Host "  Servis kaldiriliyor (nssm remove confirm)..." -ForegroundColor Yellow
        & $nssmExe remove $ServiceName confirm 2>&1 | Out-Null
        Write-Host "  [OK] Servis kaldirildi" -ForegroundColor Green
    } else {
        # NSSM yoksa SCM ile dene (eski sc create kurulumlari icin de calisir)
        Write-Host "  [UYARI] $nssmExe yok - sc.exe ile kaldirilmaya calisilacak." -ForegroundColor Yellow
        & sc.exe stop $ServiceName 2>&1 | Out-Null
        Start-Sleep -Seconds 2
        & sc.exe delete $ServiceName 2>&1 | Out-Null
        Write-Host "  [OK] sc.exe ile kaldirildi" -ForegroundColor Green
    }
}

if ($RemoveTrayShortcut) {
    $startupShortcut = Join-Path ([Environment]::GetFolderPath("Startup")) "FileActivityTray.lnk"
    if (Test-Path $startupShortcut) {
        Remove-Item $startupShortcut -Force
        Write-Host "  [OK] Tray shortcut silindi: $startupShortcut" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "  Servis kaldirildi. Dashboard'u manuel baslatmak icin:" -ForegroundColor White
Write-Host "    $InstallDir\start_dashboard.cmd" -ForegroundColor Cyan
Write-Host ""
