# Smoke tests for the FileActivity PowerShell module — issue #151 service
# control cmdlets specifically. Designed to run with or without Pester.
#
# With Pester:   Invoke-Pester -Path tests\test_powershell_cmdlets.ps1
# Without:       powershell -ExecutionPolicy Bypass -File tests\test_powershell_cmdlets.ps1
#
# Notes:
#   - Tests do NOT actually start/stop a real service. They verify the module
#     loads, the four service cmdlets are exported, and each ships
#     comment-based help. This is enough to catch the common regression of
#     forgetting to add a function to FileActivity.psd1's FunctionsToExport.
#   - Calling the cmdlets without a real "FileActivity" service installed
#     would surface a Get-Service error — that's a runtime concern, not a
#     module-shape concern, so we don't exercise it here.

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$modulePath = Join-Path $repoRoot 'powershell\FileActivity\FileActivity.psd1'

$serviceCmdlets = @(
    'Start-FileActivityService',
    'Stop-FileActivityService',
    'Restart-FileActivityService',
    'Get-FileActivityServiceStatus'
)

function Invoke-PesterSuite {
    Describe 'FileActivity service cmdlets (issue #151)' {
        BeforeAll {
            Import-Module $modulePath -Force
        }
        AfterAll {
            Remove-Module FileActivity -Force -ErrorAction SilentlyContinue
        }

        It 'Exports the four service control cmdlets' {
            $exports = (Get-Module FileActivity).ExportedFunctions.Keys
            foreach ($cmd in $serviceCmdlets) {
                $exports | Should -Contain $cmd
            }
        }

        It 'Each service cmdlet ships comment-based help' {
            foreach ($cmd in $serviceCmdlets) {
                $help = Get-Help $cmd -ErrorAction Stop
                $help.Synopsis | Should -Not -BeNullOrEmpty
            }
        }

        It 'Get-FileActivityServiceStatus returns NotInstalled instead of throwing when service absent' {
            $result = Get-FileActivityServiceStatus -ServiceName 'FileActivity-NonExistent-Smoke'
            $result.Status | Should -Be 'NotInstalled'
        }
    }
}

function Invoke-PlainSuite {
    Write-Host "[smoke] Importing module: $modulePath"
    Import-Module $modulePath -Force

    $failures = 0

    Write-Host "[smoke] Verifying service cmdlets are exported..."
    $exports = (Get-Module FileActivity).ExportedFunctions.Keys
    foreach ($cmd in $serviceCmdlets) {
        if ($exports -contains $cmd) {
            Write-Host "  [OK] $cmd"
        } else {
            Write-Host "  [FAIL] $cmd not exported" -ForegroundColor Red
            $failures++
        }
    }

    Write-Host "[smoke] Verifying comment-based help..."
    foreach ($cmd in $serviceCmdlets) {
        $help = Get-Help $cmd -ErrorAction SilentlyContinue
        if ($help -and $help.Synopsis) {
            Write-Host "  [OK] help for $cmd"
        } else {
            Write-Host "  [FAIL] no help for $cmd" -ForegroundColor Red
            $failures++
        }
    }

    Write-Host "[smoke] Verifying Get-FileActivityServiceStatus returns NotInstalled for missing service..."
    try {
        $result = Get-FileActivityServiceStatus -ServiceName 'FileActivity-NonExistent-Smoke'
        if ($result.Status -eq 'NotInstalled') {
            Write-Host "  [OK] Status=NotInstalled"
        } else {
            Write-Host "  [FAIL] expected NotInstalled, got: $($result.Status)" -ForegroundColor Red
            $failures++
        }
    } catch {
        Write-Host "  [FAIL] threw: $_" -ForegroundColor Red
        $failures++
    }

    Remove-Module FileActivity -Force -ErrorAction SilentlyContinue

    if ($failures -gt 0) {
        Write-Host "[smoke] FAILED ($failures errors)" -ForegroundColor Red
        exit 1
    }
    Write-Host "[smoke] All checks passed" -ForegroundColor Green
}

if (Get-Module -ListAvailable -Name Pester) {
    Invoke-PesterSuite
} else {
    Invoke-PlainSuite
}
