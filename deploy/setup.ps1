<#
.SYNOPSIS
    FILE ACTIVITY - Tek Komutla Kurulum
.DESCRIPTION
    GitHub'dan klonlar, bagimliliklari kurar, dizin yapisini olusturur ve dashboard'u baslatir.
    Kullanim: powershell -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1 | iex"
#>

try {

$ErrorActionPreference = "Continue"
$InstallDir = "C:\FileActivity"
$RepoUrl = "https://github.com/deepdarbe/FILE_ACTIVITY.git"
$Branch = "master"

Write-Host ""
Write-Host "  ╔══════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║  FILE ACTIVITY - Kurulum                 ║" -ForegroundColor Cyan
Write-Host "  ║  Windows File Share Analysis System      ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

function Refresh-Path {
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
    # Bilinen kurulum yollarini da ekle
    @("C:\Program Files\Git\cmd", "C:\Program Files\Git\bin",
      "C:\Program Files\Python312", "C:\Program Files\Python312\Scripts",
      "C:\Program Files\Python311", "C:\Program Files\Python311\Scripts",
      "C:\Program Files\Python310", "C:\Program Files\Python310\Scripts",
      "C:\Python312", "C:\Python312\Scripts",
      "C:\Python311", "C:\Python311\Scripts") | ForEach-Object {
        if ((Test-Path $_) -and ($env:Path -notlike "*$_*")) { $env:Path += ";$_" }
    }
}

function Download-File($url, $outPath) {
    # TLS 1.2 zorla (eski Windows Server icin gerekli)
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls

    # Yontem 1: WebClient (daha guvenilir, redirect takip eder)
    try {
        Write-Host "    Indiriliyor: $url" -ForegroundColor Gray
        $wc = New-Object System.Net.WebClient
        $wc.Headers.Add("User-Agent", "PowerShell")
        $wc.DownloadFile($url, $outPath)
        if (Test-Path $outPath) {
            $size = (Get-Item $outPath).Length
            Write-Host "    Indirildi: $([math]::Round($size/1MB, 1)) MB" -ForegroundColor Gray
            return $true
        }
    } catch {
        Write-Host "    WebClient hatasi: $_" -ForegroundColor Gray
    }

    # Yontem 2: Invoke-WebRequest
    try {
        Invoke-WebRequest -Uri $url -OutFile $outPath -UseBasicParsing -ErrorAction Stop
        if (Test-Path $outPath) { return $true }
    } catch {
        Write-Host "    IWR hatasi: $_" -ForegroundColor Gray
    }

    # Yontem 3: BITS Transfer
    try {
        Start-BitsTransfer -Source $url -Destination $outPath -ErrorAction Stop
        if (Test-Path $outPath) { return $true }
    } catch {
        Write-Host "    BITS hatasi: $_" -ForegroundColor Gray
    }

    return $false
}

function Install-WithFallback($name, $wingetId, $directUrl, $installerArgs) {
    # 1. winget dene
    $hasWinget = Get-Command winget -ErrorAction SilentlyContinue
    if ($hasWinget) {
        Write-Host "  winget ile kuruluyor..." -ForegroundColor Gray
        try {
            winget install --id $wingetId --accept-package-agreements --accept-source-agreements --silent 2>$null
            Refresh-Path
            if (Get-Command $name -ErrorAction SilentlyContinue) { return $true }
        } catch {}
    }

    # 2. Dogrudan indir ve kur
    if ($directUrl) {
        Write-Host "  Dogrudan indiriliyor..." -ForegroundColor Yellow
        $installer = "$env:TEMP\${name}_installer.exe"
        $downloaded = Download-File $directUrl $installer
        if ($downloaded) {
            Write-Host "  Kuruluyor (sessiz mod)..." -ForegroundColor Yellow
            $proc = Start-Process -FilePath $installer -ArgumentList $installerArgs -Wait -PassThru -NoNewWindow
            Write-Host "  Kurulum cikis kodu: $($proc.ExitCode)" -ForegroundColor Gray
            Remove-Item $installer -Force -ErrorAction SilentlyContinue
            Refresh-Path
            if (Get-Command $name -ErrorAction SilentlyContinue) { return $true }
            Write-Host "  [!] Kurulum tamamlandi ama '$name' PATH'te bulunamadi." -ForegroundColor Yellow
        } else {
            Write-Host "  [!] Indirme basarisiz." -ForegroundColor Red
        }
    }
    return $false
}

# ─── 1. Git kontrolu ───
Write-Host "  [1/6] Git kontrol ediliyor..." -ForegroundColor Yellow
$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
    Write-Host "  Git bulunamadi. Kuruluyor..." -ForegroundColor Yellow
    $gitUrl = "https://github.com/git-for-windows/git/releases/download/v2.47.1.windows.2/Git-2.47.1.2-64-bit.exe"
    $ok = Install-WithFallback "git" "Git.Git" $gitUrl "/VERYSILENT /NORESTART /NOCANCEL /SP- /CLOSEAPPLICATIONS /RESTARTAPPLICATIONS /COMPONENTS=`"icons,ext\reg\shellhere,assoc,assoc_sh`""
    if (-not $ok) {
        Write-Host "  [HATA] Git kurulamadi." -ForegroundColor Red
        Write-Host "  Manuel indirin: https://git-scm.com/download/win" -ForegroundColor Yellow
        Write-Host "  Kurduktan sonra bu komutu tekrar calistirin." -ForegroundColor Yellow
        Read-Host "  Devam etmek icin Enter'a basin"
        exit 1
    }
}
Write-Host "  [OK] Git: $(git --version)" -ForegroundColor Green

# ─── 2. Python kontrolu ───
Write-Host "  [2/6] Python kontrol ediliyor..." -ForegroundColor Yellow
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    Write-Host "  Python bulunamadi. Kuruluyor..." -ForegroundColor Yellow
    $pyUrl = "https://www.python.org/ftp/python/3.12.8/python-3.12.8-amd64.exe"
    $ok = Install-WithFallback "python" "Python.Python.3.12" $pyUrl "/quiet InstallAllUsers=1 PrependPath=1 Include_pip=1"
    if (-not $ok) {
        Write-Host "  [HATA] Python kurulamadi." -ForegroundColor Red
        Write-Host "  Manuel indirin: https://python.org/downloads" -ForegroundColor Yellow
        Write-Host "  Kurduktan sonra bu komutu tekrar calistirin." -ForegroundColor Yellow
        Read-Host "  Devam etmek icin Enter'a basin"
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

} catch {
    Write-Host ""
    Write-Host "  [HATA] Kurulum sirasinda bir sorun olustu:" -ForegroundColor Red
    Write-Host "  $_" -ForegroundColor Red
    Write-Host ""
}

Write-Host ""
Write-Host "  Kapatmak icin bir tusa basin..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
