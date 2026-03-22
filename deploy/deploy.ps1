<#
.SYNOPSIS
    FILE ACTIVITY - PowerShell Uzak Sunucu Dagitim Scripti

.DESCRIPTION
    Paketlenmiş FILE ACTIVITY'yi uzak sunuculara dağıtır.
    Tek sunucu veya CSV'den toplu dağıtım destekler.

.PARAMETER TargetServer
    Hedef sunucu adı veya IP

.PARAMETER TargetServers
    CSV dosyası: ServerName,InstallPath,PgHost,PgPort,PgDb,PgUser,PgPass

.PARAMETER PackagePath
    FileActivity-Package dizin yolu

.PARAMETER InstallPath
    Hedef sunucudaki kurulum dizini

.EXAMPLE
    .\deploy.ps1 -TargetServer "SRV01" -PackagePath ".\dist\FileActivity-Package"
    .\deploy.ps1 -TargetServers "servers.csv" -PackagePath ".\dist\FileActivity-Package"
#>

param(
    [string]$TargetServer,
    [string]$TargetServers,
    [string]$PackagePath = ".\dist\FileActivity-Package",
    [string]$InstallPath = "C:\FileActivity",
    [PSCredential]$Credential
)

$ErrorActionPreference = "Stop"

function Write-Step($step, $msg) {
    Write-Host "  [$step] $msg" -ForegroundColor Cyan
}

function Deploy-ToServer {
    param(
        [string]$Server,
        [string]$RemotePath,
        [hashtable]$PgConfig
    )

    Write-Host ""
    Write-Host "═══════════════════════════════════════" -ForegroundColor Yellow
    Write-Host "  Dagitim: $Server" -ForegroundColor Yellow
    Write-Host "═══════════════════════════════════════" -ForegroundColor Yellow

    # Paket kontrolu
    if (-not (Test-Path $PackagePath)) {
        Write-Host "  [HATA] Paket bulunamadi: $PackagePath" -ForegroundColor Red
        Write-Host "  Once 'build.bat' ile paket olusturun." -ForegroundColor Red
        return $false
    }

    # Uzak baglanti testi
    Write-Step "1/5" "Baglanti testi..."
    try {
        $session = if ($Credential) {
            New-PSSession -ComputerName $Server -Credential $Credential
        } else {
            New-PSSession -ComputerName $Server
        }
    } catch {
        Write-Host "  [HATA] Baglanti basarisiz: $_" -ForegroundColor Red
        return $false
    }

    try {
        # Dizin olustur
        Write-Step "2/5" "Kurulum dizini olusturuluyor: $RemotePath"
        Invoke-Command -Session $session -ScriptBlock {
            param($path)
            New-Item -Path $path -ItemType Directory -Force | Out-Null
            New-Item -Path "$path\bin" -ItemType Directory -Force | Out-Null
            New-Item -Path "$path\config" -ItemType Directory -Force | Out-Null
            New-Item -Path "$path\logs" -ItemType Directory -Force | Out-Null
            New-Item -Path "$path\scripts" -ItemType Directory -Force | Out-Null
        } -ArgumentList $RemotePath

        # Dosya transferi
        Write-Step "3/5" "Dosyalar kopyalaniyor..."
        $remoteBin = "\\$Server\$($RemotePath.Replace(':', '$'))\bin"
        $remoteConfig = "\\$Server\$($RemotePath.Replace(':', '$'))\config"
        $remoteScripts = "\\$Server\$($RemotePath.Replace(':', '$'))\scripts"

        Copy-Item -Path "$PackagePath\bin\*" -Destination $remoteBin -Recurse -Force
        Copy-Item -Path "$PackagePath\config\config.yaml" -Destination $remoteConfig -Force
        Copy-Item -Path "$PackagePath\scripts\init_db.py" -Destination $remoteScripts -Force

        # Config guncelle
        if ($PgConfig) {
            Write-Step "4/5" "PostgreSQL konfigurasyonu ayarlaniyor..."
            Invoke-Command -Session $session -ScriptBlock {
                param($path, $pg)
                $configPath = "$path\config\config.yaml"
                $content = Get-Content $configPath -Raw
                if ($pg.Host) { $content = $content -replace 'host: .*', "host: $($pg.Host)" }
                if ($pg.Port) { $content = $content -replace 'port: .*', "port: $($pg.Port)" }
                if ($pg.Db)   { $content = $content -replace 'name: .*', "name: $($pg.Db)" }
                if ($pg.User) { $content = $content -replace 'user: .*', "user: $($pg.User)" }
                if ($pg.Pass) { $content = $content -replace 'password: .*', "password: $($pg.Pass)" }
                Set-Content $configPath $content
            } -ArgumentList $RemotePath, $PgConfig
        } else {
            Write-Step "4/5" "Config: varsayilan ayarlar kullaniliyor"
        }

        # Firewall
        Write-Step "5/5" "Firewall kurali ekleniyor..."
        Invoke-Command -Session $session -ScriptBlock {
            New-NetFirewallRule -DisplayName "FileActivity Dashboard" `
                -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8085 `
                -ErrorAction SilentlyContinue | Out-Null
        }

        Write-Host "  [OK] Dagitim tamamlandi: $Server" -ForegroundColor Green
        return $true

    } catch {
        Write-Host "  [HATA] Dagitim basarisiz: $_" -ForegroundColor Red
        return $false
    } finally {
        Remove-PSSession $session -ErrorAction SilentlyContinue
    }
}

# ─── Ana Akis ───

Write-Host ""
Write-Host "  ╔══════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║  FILE ACTIVITY - Uzak Dagitim        ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════════╝" -ForegroundColor Cyan

$results = @()

if ($TargetServers -and (Test-Path $TargetServers)) {
    # CSV'den toplu dagitim
    $servers = Import-Csv $TargetServers
    Write-Host "  $($servers.Count) sunucu CSV'den yuklendi" -ForegroundColor Gray

    foreach ($srv in $servers) {
        $pg = @{
            Host = $srv.PgHost
            Port = $srv.PgPort
            Db   = $srv.PgDb
            User = $srv.PgUser
            Pass = $srv.PgPass
        }
        $remotePath = if ($srv.InstallPath) { $srv.InstallPath } else { $InstallPath }
        $ok = Deploy-ToServer -Server $srv.ServerName -RemotePath $remotePath -PgConfig $pg
        $results += [PSCustomObject]@{ Server = $srv.ServerName; Success = $ok }
    }

} elseif ($TargetServer) {
    # Tek sunucu
    $ok = Deploy-ToServer -Server $TargetServer -RemotePath $InstallPath
    $results += [PSCustomObject]@{ Server = $TargetServer; Success = $ok }

} else {
    Write-Host "  Kullanim:" -ForegroundColor Yellow
    Write-Host "    .\deploy.ps1 -TargetServer SRV01" -ForegroundColor Gray
    Write-Host "    .\deploy.ps1 -TargetServers servers.csv" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  CSV formati:" -ForegroundColor Yellow
    Write-Host "    ServerName,InstallPath,PgHost,PgPort,PgDb,PgUser,PgPass" -ForegroundColor Gray
    Write-Host "    SRV01,C:\FileActivity,dbhost,5432,file_activity,fa_user,pass123" -ForegroundColor Gray
    exit 0
}

# Ozet
Write-Host ""
Write-Host "  ═══ Dagitim Ozeti ═══" -ForegroundColor Cyan
$results | ForEach-Object {
    $color = if ($_.Success) { "Green" } else { "Red" }
    $status = if ($_.Success) { "BASARILI" } else { "BASARISIZ" }
    Write-Host "    $($_.Server): $status" -ForegroundColor $color
}
Write-Host ""
