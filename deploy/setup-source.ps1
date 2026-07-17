<#
.SYNOPSIS
    FILE ACTIVITY - Tek Komutla Kaynak Kod Kurulumu (master branch)

.DESCRIPTION
    GitHub master branch'inden kaynak kodu indirir, Python venv olusturur,
    bagimliliklari (duckdb dahil) kurar, launcher scriptleri hazirlar ve
    istege bagli olarak dashboard'u baslatir.

    EXE release'i GEREKTIRMEZ. Tek gereksinim: hedef sunucuda Python 3.10+

    Kullanim (Yonetici PowerShell). Onerilen yeni form thin entry-point
    'deploy/install.ps1' uzerinden gecer (kisadir, -Branch parametresi alir):

      powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/install.ps1 | iex"

    Eski form (geriye-uyumlu, en az bir release boyunca calismaya devam edecek):

      powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup-source.ps1 | iex"

    Bastaki TLS 1.2 atamasi, eski PowerShell 5.1 (Windows Server 2012/2016)
    varsayili TLS 1.0/1.1 kullandigi icin GitHub'a HTTPS isteginin calismasini
    garanti eder. Daha yeni sistemlerde zararsiz, guvenli tarafta kalmak icin
    kanonik komut olarak onerilir.

    Ayni komut guncelleme icin de kullanilabilir: mevcut data\, config\,
    logs\ ve reports\ dizinleri korunur, sadece kaynak kod yenilenir.

    PR/branch test etmek icin install.ps1'i -Branch parametresi ile scriptblock
    olarak cagirin (bkz. deploy/install.ps1).

.PARAMETER Branch
    Indirilecek git branch'i. Varsayilan: master. PR test ederken farkli bir
    branch (orn. claude/some-pr) verilebilir. install.ps1 bu parametreyi
    -Branch parameter binding ile aktarir.

.NOTES
    Veri korumali guncelleme: data/, logs/, reports/, config/config.yaml
    Yeniden yazilir: src/, main.py, requirements.txt, deploy/, scripts/
#>

param(
    [string]$Branch = "master"
)

$ErrorActionPreference = "Stop"

# --- Konfigurasyon ---
$InstallDir   = "C:\FileActivity"
$RepoOwner    = "deepdarbe"
$RepoName     = "FILE_ACTIVITY"
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

# Issue #172 — service-aware update. If the FileActivity Windows service
# (issue #151) is running, its nssm.exe supervisor holds an exclusive
# handle to bin\nssm.exe and the cleanup loop below fails with
# "Access to the path 'nssm.exe' is denied". Stop the service first,
# wait for nssm to release its handles, remember the state so we can
# restart it after install. Idempotent: if no service exists this is a
# no-op.
$svcWasRunning = $false
$existingSvc = Get-Service -Name "FileActivity" -ErrorAction SilentlyContinue
if ($existingSvc -and $existingSvc.Status -eq "Running") {
    Write-Host "  FileActivity servisi durduruluyor (update icin)..." -ForegroundColor Yellow
    try {
        Stop-Service -Name "FileActivity" -Force -ErrorAction Stop
        # nssm.exe needs ~1-2s after Stop-Service to fully release handles.
        # Poll up to 20s — long enough for the supervisor to die even on
        # slow disks; short enough that the operator notices a hang.
        $deadline = (Get-Date).AddSeconds(20)
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 500
            $nssmProc = Get-Process -Name "nssm" -ErrorAction SilentlyContinue
            if (-not $nssmProc) { break }
        }
        $svcWasRunning = $true
        Write-Host "  [OK] Servis durduruldu" -ForegroundColor Green
    } catch {
        Write-Host "  [UYARI] Servis durdurulamadi: $_" -ForegroundColor Yellow
        Write-Host "          Manuel: Stop-Service FileActivity, ardindan update.cmd tekrar deneyin." -ForegroundColor Yellow
        exit 1
    }
}

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

# Issue #320 — 'deploy' klasoru guncellemede kilitlenip Remove-Item'i abort
# ediyordu ("The process cannot access ... deploy ... being used by another
# process"), servis + python durdurulmus olsa bile. Iki kok sebep:
#   (1) Bu script'in (veya cocuk process'in) calisma dizini, silinecek
#       klasorun ICINDE olabiliyor — Windows, bir process'in icinde durdugu
#       dizinin silinmesine izin vermez. 'bin'/'config' silinip 'deploy'da
#       takilmasi tam bu belirti.
#   (2) Remove-Item -Recurse tek bir kilitli dosyada tum guncellemeyi patlatir.
# Cozum: CWD'yi notr (InstallDir koku) yap — boylece script silinecek hicbir
# alt dizinin icinde durmaz — ve klasorleri remove+copy yerine 'robocopy /MIR'
# ile aynala: robocopy dest dizin handle'ini SILMEDEN icerigi senkronlar ve
# kilitli dosyalari retry'lar. config\ bu dongude yok (repo kokunde config/
# dizini yok), musterinin config.yaml'i etkilenmez.
Set-Location -LiteralPath $InstallDir

function Copy-DirResilient {
    param(
        [Parameter(Mandatory)][string]$Source,
        [Parameter(Mandatory)][string]$Dest
    )
    if (-not (Test-Path $Dest)) { New-Item -Path $Dest -ItemType Directory -Force | Out-Null }
    # /MIR: aynala (ekle/guncelle + eskiyeni sil). /R:3 /W:2: kilitli dosya retry.
    # Loglar sessiz. robocopy her Windows'ta System32'de bulunur.
    & robocopy $Source $Dest /MIR /NFL /NDL /NJH /NJS /NP /R:3 /W:2 | Out-Null
    # robocopy exit kodlari: 0-7 basari (bit-flag; 1=kopyalandi, 2=fazla vs.),
    # 8+ gercek hata.
    if ($LASTEXITCODE -ge 8) {
        # Son care: remove+copy'yi ustel backoff ile birkac kez dene.
        for ($i = 0; $i -lt 3; $i++) {
            try {
                if (Test-Path $Dest) { Remove-Item $Dest -Recurse -Force -ErrorAction Stop }
                Copy-Item -Path $Source -Destination $Dest -Recurse -Force -ErrorAction Stop
                $global:LASTEXITCODE = 0
                return
            } catch {
                Start-Sleep -Seconds ([int][Math]::Pow(2, $i))
            }
        }
        throw "Klasor guncellenemedi: $Dest — icindeki bir dosya/dizin kilitli. Ilgili pencereyi/process'i kapatip update.cmd'yi tekrar deneyin."
    }
    $global:LASTEXITCODE = 0
}

Get-ChildItem $srcRoot -Force | Where-Object { $skipTop -notcontains $_.Name } | ForEach-Object {
    $dest = "$InstallDir\$($_.Name)"
    if ($_.PSIsContainer) {
        Copy-DirResilient -Source $_.FullName -Dest $dest
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

# Update launcher: thin install.ps1 entry-point'unu tekrar cagirir.
# Eski PowerShell'lerde TLS 1.2'yi onceden set etmek zorunlu (irm basarisiz olmasin)
# Issue #77: update'ten ONCE SQLite snapshot al — guncelleme bozulursa
# operator hizlica geri donebilir. Snapshot basarisiz olsa bile update
# devam eder (snapshot olmadan da olabilir, ama update durmamali).
# --skip-if-recent-minutes 30: ayni 30 dakika icinde bir snapshot
# zaten alinmissa yeniden alma — 3M dosyali bir DB'de VACUUM INTO yerine
# online-backup'a gectik ama yine de 2-3 GB'lik yazma var, gereksiz
# yere her update'te tekrarlamak operator'i bekletiyordu.
#
# Branch'i koru: install.ps1 -Branch parametresi alir, scriptblock-Create
# pattern ile thread eder (eski temp-file dance gerekmez).
# #351/#362: 2-3 GB'lik snapshot her update'te operator'i bekletiyordu. Artik
# update.cmd basinda bir E/H sorusu var. HIZLI VARSAYILAN (operator talebi):
# timeout (10sn) VE explicit "H" yedegi ATLAR; yedek istiyorsan "E" bas.
# ONEMLI GUVENLIK RAYI KORUNDU: choice.exe bulunamazsa (errorlevel 9009) ya da
# beklenmedik bir hata olursa yine YEDEK ALINIR -> `if "%errorlevel%"=="2"`
# (tam esitlik), `if errorlevel 2` DEGIL: ikincisi 9009'u da yakalayip yedegi
# atlardi (arac bozukken guvensiz yon). Yani "prompt calisiyorsa hizli-atla,
# prompt bozuksa guvenli-yedekle". Snapshot zaten --skip-if-recent-minutes 30
# ile son 30dk icinde alinmissa kendiliginden ~aninda gecer.
$updateCmd = @"
@echo off
echo FILE ACTIVITY guncelleniyor ($Branch branch)...
cd /d "$InstallDir"
echo.
choice /C EH /N /T 10 /D H /M "Update oncesi SQLite yedegi alinsin mi? (E=Evet, yedek al / H=Hayir, ATLA [varsayilan, 10sn]): "
if "%errorlevel%"=="2" goto fa_skipbackup
echo  - Pre-update SQLite snapshot aliniyor (son 30dk icindeyse atlanir)...
"$InstallDir\.venv\Scripts\python.exe" -m src.storage.backup_manager snapshot --reason "update" --skip-if-recent-minutes 30
if errorlevel 1 (
    echo  [!] Snapshot basarisiz - update yine de devam ediyor
)
goto fa_doupdate
:fa_skipbackup
echo  - [ATLANDI] Yedek alinmadan devam ediliyor (secim: Hayir).
:fa_doupdate
powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; & ([scriptblock]::Create((irm https://raw.githubusercontent.com/$RepoOwner/$RepoName/$Branch/deploy/install.ps1))) -Branch $Branch"
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

# --- Config migration (issue #194 Wave 8 / D7) ---
# setup-source.ps1 preserves the customer's config.yaml across updates so
# operator customisations survive. But that means any new safe-default we
# ship (e.g. parquet_staging.enabled: false from PR #174) never reaches
# the customer's machine — they keep running the old unsafe value.
# scripts/migrate_config.py flips ONLY keys whose current value matches
# the documented pre-flip default; any operator-chosen value is left
# alone. Backup is mandatory before any write.
$migratorPy   = Join-Path $InstallDir 'scripts\migrate_config.py'
$customerCfg  = Join-Path $InstallDir 'config\config.yaml'
if ((Test-Path $migratorPy) -and (Test-Path $customerCfg) -and (Test-Path $venvPy)) {
    Write-Host ""
    Write-Host "Config flag-rot migrator (PR #194 / D7)..." -ForegroundColor Yellow
    try {
        & $venvPy $migratorPy $customerCfg
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  [OK] config migrator basarili" -ForegroundColor Green
        } else {
            Write-Host "  [UYARI] config migrator hata kodu $LASTEXITCODE — kontrol edin: $InstallDir\logs\" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  [UYARI] config migrator calistirilamadi: $_" -ForegroundColor Yellow
    }
}

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

# Issue #172 — if we stopped the service before cleanup, just restart
# it now. We skip the install prompt entirely: the service already
# exists with its NSSM config, all we need is the new code to run.
if ($svcWasRunning) {
    Write-Host ""
    Write-Host "  FileActivity servisi yeniden baslatiliyor (yeni kod ile)..." -ForegroundColor Yellow
    try {
        Start-Service -Name "FileActivity" -ErrorAction Stop
        # Give it a moment to actually transition to Running. Same poll
        # budget as install_service.ps1 [4/4] post-#163.
        $deadline = (Get-Date).AddSeconds(15)
        do {
            Start-Sleep -Seconds 1
            $svcNow = Get-Service -Name "FileActivity" -ErrorAction SilentlyContinue
        } while ((Get-Date) -lt $deadline -and $svcNow.Status -ne "Running")
        if ($svcNow -and $svcNow.Status -eq "Running") {
            Write-Host "  [OK] Servis calisiyor" -ForegroundColor Green
        } else {
            $statusText = if ($svcNow) { $svcNow.Status } else { "<bulunamadi>" }
            Write-Host "  [UYARI] Servis Running degil. Status: $statusText" -ForegroundColor Yellow
            Write-Host "          Loglara bakin: $InstallDir\logs\service.err" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  [UYARI] Servis baslatilamadi: $_" -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host "  Update tamamlandi. Dashboard: http://localhost:$DashPort" -ForegroundColor Yellow
    Write-Host ""
    return
}

# --- Issue #151: Servis modu (NSSM) opt-in ---
# Default H to preserve current behavior; user must explicitly choose service mode.
Write-Host ""
Write-Host "  Hizmet olarak yuklensin mi (Windows Service, otomatik baslatma + crash recovery)?" -ForegroundColor White
Write-Host "  [E] Evet  [H] Hayir (sadece manuel start_dashboard.cmd ile)" -ForegroundColor White
$svcAnswer = Read-Host "  Secim (E/H) [H]"
$serviceInstalled = $false
if ($svcAnswer -eq "E" -or $svcAnswer -eq "e") {
    $svcScript = Join-Path $InstallDir "deploy\install_service.ps1"
    if (Test-Path $svcScript) {
        Write-Host "  Servis kuruluyor (install_service.ps1)..." -ForegroundColor Cyan
        & powershell -ExecutionPolicy Bypass -File $svcScript -InstallDir $InstallDir
        if ($LASTEXITCODE -eq 0) { $serviceInstalled = $true }
    } else {
        Write-Host "  [UYARI] $svcScript bulunamadi - servis modu atlandi." -ForegroundColor Yellow
    }
}

# --- Issue #151: Sistem tepsisi (tray) opt-in (servis kuruluysa anlamli) ---
if ($serviceInstalled) {
    Write-Host ""
    Write-Host "  Sistem tepsisi simgesi yuklensin mi (durum gostergesi + tek tikla yeniden baslat)?" -ForegroundColor White
    Write-Host "  [E] Evet  [H] Hayir" -ForegroundColor White
    $trayAnswer = Read-Host "  Secim (E/H) [H]"
    if ($trayAnswer -eq "E" -or $trayAnswer -eq "e") {
        $trayScript = Join-Path $InstallDir "deploy\install_tray.ps1"
        if (Test-Path $trayScript) {
            & powershell -ExecutionPolicy Bypass -File $trayScript -InstallDir $InstallDir
        } else {
            Write-Host "  [UYARI] $trayScript bulunamadi - tray atlandi." -ForegroundColor Yellow
        }
    }
}

# --- Otomatik baslatma (sadece servis modu secilmediyse) ---
if ($serviceInstalled) {
    Write-Host ""
    Write-Host "  Servis kurulu - dashboard zaten arka planda calisiyor." -ForegroundColor Green
    Write-Host "  Tarayicida acmak icin: http://localhost:$DashPort" -ForegroundColor Yellow
    try {
        Start-Process "http://localhost:$DashPort"
    } catch {}
} else {
    $answer = Read-Host "  Dashboard simdi baslasin mi? (E/H) [E]"
    if ($answer -ne "H" -and $answer -ne "h") {
        Write-Host "  Dashboard baslatiliyor..." -ForegroundColor Cyan
        Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "`"$InstallDir\start_dashboard.cmd`"" -WindowStyle Normal
        Start-Sleep -Seconds 3
        Start-Process "http://localhost:$DashPort"
        Write-Host "  [OK] Dashboard baslatildi. Tarayici acilmadiysa: http://localhost:$DashPort" -ForegroundColor Green
    }
}
Write-Host ""
