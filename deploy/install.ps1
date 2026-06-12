<#
.SYNOPSIS
    FILE ACTIVITY - Thin one-liner entry point. Downloads setup-source.ps1
    for the requested branch and invokes it with parameter binding.

.DESCRIPTION
    setup-source.ps1 is the heavy lifter (Python install, venv, deps,
    launcher generation, optional service install). install.ps1 is the
    tiny front door:

      1. Forces TLS 1.2 (Windows Server 2012/2016 PowerShell 5.1 still
         defaults to TLS 1.0/1.1 which GitHub rejects).
      2. Fetches setup-source.ps1 for the requested branch via irm.
      3. Invokes it as a script block with -Branch threaded through, so
         a PR branch can be tested without the awkward temp-file dance
         that 'irm | iex' forces (iex can't take arguments).

    Idempotent: same one-liner handles both fresh install AND update.
    setup-source.ps1 preserves data/, logs/, reports/, config/config.yaml.

    Kullanim (Admin PowerShell):

      # Standart kurulum / update (master):
      powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/install.ps1 | iex"

      # PR/branch test (-Branch parametresi):
      powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; & ([scriptblock]::Create((irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/install.ps1))) -Branch claude/some-pr"

.PARAMETER Branch
    Indirilecek git branch. Varsayilan: master. PR test ederken yaz.

.NOTES
    PowerShell 5.1 uyumlu (no PS7-only syntax — no ternary, no null-coalescing).
    Service'a hic dokunmaz; sadece setup-source.ps1'i indirir ve cagirir.
#>

param(
    [string]$Branch = "master"
)

$ErrorActionPreference = "Stop"

# --- Konfigurasyon ---
$RepoOwner = "deepdarbe"
$RepoName  = "FILE_ACTIVITY"
$SetupUrl  = "https://raw.githubusercontent.com/$RepoOwner/$RepoName/$Branch/deploy/setup-source.ps1"

Write-Host ""
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host "  |  FILE ACTIVITY - install.ps1 entry point |" -ForegroundColor Cyan
Write-Host "  |  branch: $Branch" -ForegroundColor Cyan
Write-Host "  +==========================================+" -ForegroundColor Cyan
Write-Host ""

# --- TLS 1.2 (eski PowerShell 5.1 icin sart) ---
# Idempotent — caller'in zaten set ettigi durumda no-op olur. install.ps1
# bir scriptblock olarak cagrildiginda caller'in TLS ayari kaybolabilir,
# burada da set etmek dogru.
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# --- setup-source.ps1'i indir + parameter binding ile cagir ---
Write-Host "  setup-source.ps1 indiriliyor ($Branch)..." -ForegroundColor Yellow
try {
    $setupScript = Invoke-RestMethod -Uri $SetupUrl -UseBasicParsing
} catch {
    Write-Host "  [HATA] setup-source.ps1 indirilemedi: $_" -ForegroundColor Red
    Write-Host "         URL: $SetupUrl" -ForegroundColor Yellow
    Write-Host "         Branch dogru mu? Internet/proxy erisimi var mi?" -ForegroundColor Yellow
    exit 1
}

# scriptblock-Create pattern: irm'den gelen string'i scriptblock'a cevir,
# -Branch parametresini parameter binding ile aktar. iex'ten farkli olarak
# bu form argument kabul eder.
$sb = [scriptblock]::Create($setupScript)
& $sb -Branch $Branch
exit $LASTEXITCODE
