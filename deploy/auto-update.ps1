<#
.SYNOPSIS
    FILE ACTIVITY - GitHub'dan Otomatik Guncelleme Scripti

.DESCRIPTION
    GitHub reposundan son surumu ceker, veritabani/config koruyarak gunceller.
    Windows Task Scheduler ile periyodik calistirilabilir.

.PARAMETER InstallDir
    Kurulum dizini (varsayilan: C:\FileActivity)

.PARAMETER Branch
    GitHub branch (varsayilan: master)

.PARAMETER RepoUrl
    GitHub repo URL (varsayilan: deepdarbe/FILE_ACTIVITY)

.PARAMETER Mode
    source: Python kaynak kod ile calistir
    exe: PyInstaller EXE ile calistir (varsayilan: source)

.EXAMPLE
    .\auto-update.ps1
    .\auto-update.ps1 -InstallDir "D:\FileActivity" -Branch "main"
    .\auto-update.ps1 -Mode exe
#>

param(
    [string]$InstallDir = "C:\FileActivity",
    [string]$Branch = "master",
    [string]$RepoUrl = "https://github.com/deepdarbe/FILE_ACTIVITY.git",
    [string]$Mode = "source",
    [switch]$Force,
    [switch]$SetupSchedule
)

$ErrorActionPreference = "Stop"
$LogFile = "$InstallDir\logs\auto-update.log"

function Write-Log($msg, $level = "INFO") {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] [$level] $msg"
    Write-Host $line -ForegroundColor $(switch($level) { "ERROR" {"Red"} "WARN" {"Yellow"} "OK" {"Green"} default {"Cyan"} })
    if (Test-Path (Split-Path $LogFile)) {
        Add-Content -Path $LogFile -Value $line -ErrorAction SilentlyContinue
    }
}

# ═══════════════════════════════════════════
# ZAMANLANMIS GOREV KURULUMU
# ═══════════════════════════════════════════
if ($SetupSchedule) {
    Write-Host ""
    Write-Host "  FILE ACTIVITY - Otomatik Guncelleme Zamanlama" -ForegroundColor Cyan
    Write-Host "  ==============================================" -ForegroundColor Cyan
    Write-Host ""

    $scriptPath = $MyInvocation.MyCommand.Path
    if (-not $scriptPath) { $scriptPath = "$InstallDir\deploy\auto-update.ps1" }

    $action = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -InstallDir `"$InstallDir`""

    # Her gun saat 03:00'te kontrol et
    $trigger = New-ScheduledTaskTrigger -Daily -At "03:00"

    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable

    $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

    Register-ScheduledTask `
        -TaskName "FileActivity_AutoUpdate" `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description "FILE ACTIVITY - GitHub'dan otomatik guncelleme kontrolu (gunluk 03:00)" `
        -Force

    Write-Host "  [OK] Zamanlanmis gorev olusturuldu: FileActivity_AutoUpdate" -ForegroundColor Green
    Write-Host "  Her gun 03:00'te GitHub'dan guncelleme kontrol edilecek." -ForegroundColor Gray
    Write-Host ""
    exit 0
}

# ═══════════════════════════════════════════
# GUNCELLEME KONTROLU VE UYGULAMA
# ═══════════════════════════════════════════

Write-Host ""
Write-Host "  ╔══════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║  FILE ACTIVITY - Auto Update         ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# 1. Git kontrolu
$gitPath = (Get-Command git -ErrorAction SilentlyContinue).Source
if (-not $gitPath) {
    Write-Log "Git bulunamadi! Git kurulumu gerekli: https://git-scm.com" "ERROR"
    exit 1
}

# 2. Kurulum dizini kontrolu
if (-not (Test-Path $InstallDir)) {
    Write-Log "Kurulum dizini bulunamadi. Ilk kurulum yapiliyor: $InstallDir" "INFO"

    # Dizin yapisi olustur
    $dirs = @("config", "data", "logs", "reports", "scripts", "deploy")
    New-Item -Path $InstallDir -ItemType Directory -Force | Out-Null
    foreach ($d in $dirs) {
        New-Item -Path "$InstallDir\$d" -ItemType Directory -Force | Out-Null
    }

    # Repo'yu clone et
    Write-Log "GitHub'dan klonlaniyor: $RepoUrl" "INFO"
    git clone --branch $Branch --single-branch $RepoUrl "$InstallDir\repo" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Git clone basarisiz!" "ERROR"
        exit 1
    }

    # Config kopyala
    if (-not (Test-Path "$InstallDir\config\config.yaml")) {
        Copy-Item "$InstallDir\repo\config.yaml" "$InstallDir\config\config.yaml"
        Write-Log "Varsayilan config.yaml olusturuldu" "OK"
    }

    # Config'de database yolunu guncelle
    $configPath = "$InstallDir\config\config.yaml"
    $content = Get-Content $configPath -Raw
    $content = $content -replace 'path: "data/file_activity.db"', "path: `"$InstallDir\data\file_activity.db`""
    Set-Content $configPath $content

    Write-Log "Ilk kurulum tamamlandi!" "OK"
    Write-Log "Baslatmak icin: python $InstallDir\repo\main.py dashboard --config $InstallDir\config\config.yaml" "INFO"
    exit 0
}

# 3. Repo var mi kontrol et, yoksa clone et
$repoDir = "$InstallDir\repo"
if (-not (Test-Path "$repoDir\.git")) {
    Write-Log "Repo dizini bulunamadi, klonlaniyor..." "INFO"
    if (Test-Path $repoDir) { Remove-Item $repoDir -Recurse -Force }
    git clone --branch $Branch --single-branch $RepoUrl $repoDir 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Git clone basarisiz!" "ERROR"
        exit 1
    }
    Write-Log "Repo klonlandi" "OK"
}

# 4. Mevcut surum kontrolu
Set-Location $repoDir
$localHash = (git rev-parse HEAD 2>&1).Trim()
Write-Log "Mevcut surum: $($localHash.Substring(0,8))" "INFO"

# 5. Uzak repo'dan guncelleme kontrol et
Write-Log "GitHub'dan guncelleme kontrol ediliyor..." "INFO"
git fetch origin $Branch 2>&1
$remoteHash = (git rev-parse "origin/$Branch" 2>&1).Trim()
Write-Log "GitHub surumu: $($remoteHash.Substring(0,8))" "INFO"

if ($localHash -eq $remoteHash -and -not $Force) {
    Write-Log "Sistem guncel - guncelleme gerekmiyor." "OK"
    exit 0
}

# 6. Guncelleme var! Uygula
$commitsBehind = (git log --oneline "$localHash..$remoteHash" 2>&1 | Measure-Object).Count
Write-Log "GUNCELLEME BULUNDU! $commitsBehind yeni commit." "WARN"

# 6a. Son degisiklikleri listele
Write-Log "Degisiklikler:" "INFO"
git log --oneline "$localHash..$remoteHash" 2>&1 | ForEach-Object {
    Write-Log "  $_" "INFO"
}

# 7. Servisi/dashboard'u durdur
Write-Log "Dashboard durduruluyor..." "INFO"
$service = Get-Service -Name "FileActivityService" -ErrorAction SilentlyContinue
if ($service -and $service.Status -eq "Running") {
    Stop-Service "FileActivityService" -Force -ErrorAction SilentlyContinue
    Write-Log "Servis durduruldu" "OK"
    $wasService = $true
} else {
    Get-Process -Name "FileActivity" -ErrorAction SilentlyContinue | Stop-Process -Force
    Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -like "*main.py*dashboard*"
    } | Stop-Process -Force -ErrorAction SilentlyContinue
    $wasService = $false
}
Start-Sleep -Seconds 2

# 8. Git pull (kaynak kodu guncelle)
Write-Log "Kaynak kod guncelleniyor..." "INFO"
git reset --hard "origin/$Branch" 2>&1
git clean -fd 2>&1
Write-Log "Kaynak kod guncellendi" "OK"

# 9. Python bagimliliklar guncelle
if ($Mode -eq "source") {
    Write-Log "Python bagimliliklari kontrol ediliyor..." "INFO"
    $pip = (Get-Command pip -ErrorAction SilentlyContinue).Source
    if ($pip) {
        pip install -r "$repoDir\requirements.txt" --quiet 2>&1
        Write-Log "Bagimliliklar guncellendi" "OK"
    }
}

# 10. EXE modu: yeniden build et
if ($Mode -eq "exe") {
    Write-Log "EXE yeniden derleniyor..." "INFO"
    Set-Location $repoDir
    & "$repoDir\build.bat" 2>&1
    if (Test-Path "$repoDir\dist\FileActivity\FileActivity.exe") {
        # Eski bin yedekle
        if (Test-Path "$InstallDir\bin") {
            if (Test-Path "$InstallDir\bin_old") { Remove-Item "$InstallDir\bin_old" -Recurse -Force }
            Rename-Item "$InstallDir\bin" "bin_old"
        }
        Copy-Item "$repoDir\dist\FileActivity\*" "$InstallDir\bin\" -Recurse -Force
        Write-Log "EXE guncellendi" "OK"
    } else {
        Write-Log "EXE build basarisiz! Eski surum korunuyor." "ERROR"
    }
}

# 11. Servisi/dashboard'u yeniden baslat
Write-Log "Dashboard baslatiliyor..." "INFO"
if ($wasService) {
    Start-Service "FileActivityService" -ErrorAction SilentlyContinue
    Write-Log "Servis yeniden baslatildi" "OK"
} else {
    if ($Mode -eq "source") {
        $python = (Get-Command python -ErrorAction SilentlyContinue).Source
        if ($python) {
            $configPath = "$InstallDir\config\config.yaml"
            if (-not (Test-Path $configPath)) { $configPath = "$repoDir\config.yaml" }
            Start-Process -FilePath $python `
                -ArgumentList "$repoDir\main.py", "dashboard", "--config", $configPath `
                -WindowStyle Hidden
            Write-Log "Dashboard baslatildi (source mode)" "OK"
        }
    } else {
        if (Test-Path "$InstallDir\bin\FileActivity.exe") {
            Start-Process -FilePath "$InstallDir\bin\FileActivity.exe" `
                -ArgumentList "dashboard", "--config", "$InstallDir\config\config.yaml" `
                -WindowStyle Hidden
            Write-Log "Dashboard baslatildi (EXE mode)" "OK"
        }
    }
}

# 12. Ozet
Write-Host ""
Write-Host "  ========================================" -ForegroundColor Green
Write-Host "   Guncelleme Tamamlandi!" -ForegroundColor Green
Write-Host "  ========================================" -ForegroundColor Green
Write-Host ""
Write-Host "   Eski: $($localHash.Substring(0,8))" -ForegroundColor Gray
Write-Host "   Yeni: $($remoteHash.Substring(0,8))" -ForegroundColor Gray
Write-Host "   Commits: $commitsBehind" -ForegroundColor Gray
Write-Host ""
Write-Host "   Korunan:" -ForegroundColor Yellow
Write-Host "     - data\file_activity.db (veritabani)" -ForegroundColor Gray
Write-Host "     - config\config.yaml (ayarlar)" -ForegroundColor Gray
Write-Host "     - logs\ (log dosyalari)" -ForegroundColor Gray
Write-Host "     - reports\ (raporlar)" -ForegroundColor Gray
Write-Host ""
Write-Log "Guncelleme tamamlandi: $($localHash.Substring(0,8)) -> $($remoteHash.Substring(0,8))" "OK"
