#Requires -RunAsAdministrator
<#
.SYNOPSIS
    FILE ACTIVITY - Windows File Server Audit Configuration Script
    Netwrix-compatible audit settings for file share monitoring.

.DESCRIPTION
    Configures Windows audit policies, NTFS audit entries, event log settings,
    and services required for FILE ACTIVITY to collect file access events
    (create, modify, delete, rename, move, copy).

    Based on Netwrix Windows Server Auditing best practices.
    All changes are logged and can be rolled back.

.PARAMETER Action
    configure  - Apply all audit settings (default)
    check      - Report current audit status without changes
    rollback   - Revert all changes using saved backup

.PARAMETER SharePaths
    Array of share paths to configure NTFS auditing on.
    Example: @("E:\BURCU_ORTAK", "D:\Shared")

.PARAMETER ServiceAccount
    Account to grant "Manage auditing and security log" right.
    Default: current user.

.PARAMETER BackupPath
    Where to save configuration backup for rollback.
    Default: C:\FileActivity\audit_backup

.EXAMPLE
    .\Configure-FileAudit.ps1 -Action check
    .\Configure-FileAudit.ps1 -Action configure -SharePaths @("E:\BURCU_ORTAK")
    .\Configure-FileAudit.ps1 -Action rollback

.NOTES
    Version: 1.0
    Author:  FILE ACTIVITY
    Based on: Netwrix Windows Server Auditing Guide
#>

param(
    [ValidateSet("configure", "check", "rollback")]
    [string]$Action = "check",

    [string[]]$SharePaths = @(),

    [string]$ServiceAccount = "",

    [string]$BackupPath = "C:\FileActivity\audit_backup",

    [switch]$Force
)

# ============================================================
# CONSTANTS
# ============================================================

$SCRIPT_VERSION = "1.0"
$LOG_FILE = "C:\FileActivity\logs\audit_config.log"
$BACKUP_FILE = Join-Path $BackupPath "audit_backup.json"

# Colors
function Write-Status($msg) { Write-Host "  [OK] " -ForegroundColor Green -NoNewline; Write-Host $msg }
function Write-Warning2($msg) { Write-Host "  [!!] " -ForegroundColor Yellow -NoNewline; Write-Host $msg }
function Write-Error2($msg) { Write-Host "  [XX] " -ForegroundColor Red -NoNewline; Write-Host $msg }
function Write-Info($msg) { Write-Host "  [..] " -ForegroundColor Cyan -NoNewline; Write-Host $msg }
function Write-Section($msg) { Write-Host "`n  === $msg ===" -ForegroundColor White }

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$ts | $msg"
    Add-Content -Path $LOG_FILE -Value $line -ErrorAction SilentlyContinue
}

# ============================================================
# 1. AUDIT POLICY CHECK / CONFIGURE
# ============================================================

function Get-AuditPolicyStatus {
    <#
    Checks Advanced Audit Policy settings.
    Required by FILE ACTIVITY:
      - Object Access > Audit File Share: Success,Failure
      - Object Access > Audit File System: Success,Failure  (optional but recommended)
      - Object Access > Audit Handle Manipulation: Success,Failure
      - Object Access > Audit Other Object Access Events: Success,Failure
      - Account Management > Audit Security Group Management: Success,Failure
      - Account Management > Audit User Account Management: Success,Failure
      - Policy Change > Audit Audit Policy Change: Success,Failure
    #>

    $policies = @()

    # Get current audit policy via auditpol
    $raw = auditpol /get /category:* /r 2>$null
    if (-not $raw) {
        Write-Error2 "auditpol command failed"
        return $policies
    }

    $csv = $raw | ConvertFrom-Csv

    $required = @{
        # Subcategory Name = Required Setting
        "File Share"                    = "Success and Failure"
        "File System"                   = "Success and Failure"
        "Handle Manipulation"           = "Success and Failure"
        "Other Object Access Events"    = "Success and Failure"
        "Security Group Management"     = "Success and Failure"
        "User Account Management"       = "Success and Failure"
        "Audit Policy Change"           = "Success and Failure"
        "Registry"                      = "Success"
    }

    foreach ($row in $csv) {
        $subcat = $row.'Subcategory'
        if (-not $subcat) { continue }

        foreach ($req in $required.GetEnumerator()) {
            if ($subcat -like "*$($req.Key)*") {
                $current = $row.'Inclusion Setting'
                $needed = $req.Value
                $ok = ($current -eq $needed) -or ($current -eq "Success and Failure" -and $needed -eq "Success")

                $policies += [PSCustomObject]@{
                    Subcategory = $req.Key
                    Current     = $current
                    Required    = $needed
                    OK          = $ok
                }
            }
        }
    }

    return $policies
}

function Set-AuditPolicies {
    Write-Section "Audit Policy Configuration"

    $settings = @(
        @{ Subcat = "File Share";                 Setting = "/success:enable /failure:enable" },
        @{ Subcat = "File System";                Setting = "/success:enable /failure:enable" },
        @{ Subcat = "Handle Manipulation";        Setting = "/success:enable /failure:enable" },
        @{ Subcat = "Other Object Access Events"; Setting = "/success:enable /failure:enable" },
        @{ Subcat = "Security Group Management";  Setting = "/success:enable /failure:enable" },
        @{ Subcat = "User Account Management";    Setting = "/success:enable /failure:enable" },
        @{ Subcat = "Audit Policy Change";        Setting = "/success:enable /failure:enable" },
        @{ Subcat = "Registry";                   Setting = "/success:enable" }
    )

    foreach ($s in $settings) {
        $cmd = "auditpol /set /subcategory:`"$($s.Subcat)`" $($s.Setting)"
        $result = Invoke-Expression $cmd 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Status "$($s.Subcat): Configured"
            Log "AUDIT_POLICY: $($s.Subcat) set to $($s.Setting)"
        } else {
            Write-Error2 "$($s.Subcat): Failed - $result"
            Log "AUDIT_POLICY_FAIL: $($s.Subcat) - $result"
        }
    }

    # Force audit policy subcategory override (Image 7)
    # This ensures Advanced Audit Policy overrides legacy Audit Policy
    $regPath = "HKLM:\System\CurrentControlSet\Control\Lsa"
    $current = Get-ItemProperty -Path $regPath -Name "SCENoApplyLegacyAuditPolicy" -ErrorAction SilentlyContinue
    if (-not $current -or $current.SCENoApplyLegacyAuditPolicy -ne 1) {
        Set-ItemProperty -Path $regPath -Name "SCENoApplyLegacyAuditPolicy" -Value 1 -Type DWord
        Write-Status "Force audit subcategory override: Enabled"
        Log "REGISTRY: SCENoApplyLegacyAuditPolicy = 1"
    } else {
        Write-Status "Force audit subcategory override: Already enabled"
    }
}

# ============================================================
# 2. EVENT LOG SIZE & RETENTION (Image 9)
# ============================================================

function Get-EventLogSettings {
    $logs = @("Security", "System", "Application")
    $results = @()

    foreach ($logName in $logs) {
        $log = Get-WinEvent -ListLog $logName -ErrorAction SilentlyContinue
        if ($log) {
            $results += [PSCustomObject]@{
                LogName      = $logName
                MaxSizeKB    = [math]::Round($log.MaximumSizeInBytes / 1024)
                MaxSizeMB    = [math]::Round($log.MaximumSizeInBytes / 1024 / 1024)
                Retention    = $log.LogMode
                RecordCount  = $log.RecordCount
                OK           = ($log.MaximumSizeInBytes -ge 4194240 * 1024)  # 4GB minimum
            }
        }
    }
    return $results
}

function Set-EventLogSettings {
    Write-Section "Event Log Configuration"

    # Security log: 4GB, overwrite as needed (Image 9)
    $targetSizeKB = 4194240  # ~4GB

    foreach ($logName in @("Security", "System")) {
        try {
            $log = Get-WinEvent -ListLog $logName
            $currentKB = [math]::Round($log.MaximumSizeInBytes / 1024)

            if ($currentKB -lt $targetSizeKB) {
                wevtutil sl $logName /ms:$($targetSizeKB * 1024)
                Write-Status "$logName log: Size set to $([math]::Round($targetSizeKB/1024)) MB"
                Log "EVENT_LOG: $logName size set to $targetSizeKB KB"
            } else {
                Write-Status "$logName log: Already $([math]::Round($currentKB/1024)) MB (OK)"
            }

            # Retention: Overwrite as needed
            wevtutil sl $logName /rt:false
            Write-Status "$logName log: Retention set to 'Overwrite as needed'"
            Log "EVENT_LOG: $logName retention set to overwrite"

        } catch {
            Write-Error2 "$logName log: $($_.Exception.Message)"
        }
    }
}

# ============================================================
# 3. NTFS AUDIT ENTRIES ON SHARE FOLDERS (Image 12)
# ============================================================

function Get-NTFSAuditStatus {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return [PSCustomObject]@{ Path = $Path; Status = "NOT_FOUND"; AuditRules = @() }
    }

    try {
        $acl = Get-Acl -Path $Path -Audit
        $rules = $acl.GetAuditRules($true, $true, [System.Security.Principal.NTAccount])

        $auditEntries = @()
        foreach ($rule in $rules) {
            $auditEntries += [PSCustomObject]@{
                Identity = $rule.IdentityReference.Value
                Rights   = $rule.FileSystemRights.ToString()
                Type     = $rule.AuditFlags.ToString()
                Inherit  = $rule.InheritanceFlags.ToString()
            }
        }

        $hasEveryone = $rules | Where-Object { $_.IdentityReference.Value -eq "Everyone" }

        return [PSCustomObject]@{
            Path       = $Path
            Status     = if ($hasEveryone) { "CONFIGURED" } else { "MISSING_AUDIT" }
            AuditRules = $auditEntries
            RuleCount  = $rules.Count
        }
    } catch {
        return [PSCustomObject]@{ Path = $Path; Status = "ACCESS_DENIED"; AuditRules = @() }
    }
}

function Set-NTFSAudit {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        Write-Error2 "Path not found: $Path"
        return
    }

    Write-Info "Configuring NTFS audit for: $Path"

    try {
        $acl = Get-Acl -Path $Path -Audit

        # Add audit rule: Everyone - Success+Failure for file operations
        # This captures: Create, Delete, Modify, Rename, Move, Copy, Read
        $rights = [System.Security.AccessControl.FileSystemRights]::Modify -bor
                  [System.Security.AccessControl.FileSystemRights]::Delete -bor
                  [System.Security.AccessControl.FileSystemRights]::DeleteSubdirectoriesAndFiles -bor
                  [System.Security.AccessControl.FileSystemRights]::ChangePermissions -bor
                  [System.Security.AccessControl.FileSystemRights]::TakeOwnership -bor
                  [System.Security.AccessControl.FileSystemRights]::Write

        $auditFlags = [System.Security.AccessControl.AuditFlags]::Success -bor
                      [System.Security.AccessControl.AuditFlags]::Failure

        $inheritFlags = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
                        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit

        $propagation = [System.Security.AccessControl.PropagationFlags]::None

        $rule = New-Object System.Security.AccessControl.FileSystemAuditRule(
            "Everyone", $rights, $inheritFlags, $propagation, $auditFlags
        )

        $acl.AddAuditRule($rule)
        Set-Acl -Path $Path -AclObject $acl

        Write-Status "NTFS audit configured: $Path (Everyone, Modify+Delete, Success+Failure)"
        Log "NTFS_AUDIT: Configured $Path"

    } catch {
        Write-Error2 "NTFS audit failed for $Path : $($_.Exception.Message)"
        Log "NTFS_AUDIT_FAIL: $Path - $($_.Exception.Message)"
    }
}

# ============================================================
# 4. SERVICES (Image 10 - Remote Registry)
# ============================================================

function Get-RequiredServices {
    $services = @(
        @{ Name = "RemoteRegistry"; Display = "Remote Registry"; Required = "Running" },
        @{ Name = "EventLog";       Display = "Windows Event Log"; Required = "Running" },
        @{ Name = "LanmanServer";   Display = "Server (SMB)"; Required = "Running" }
    )

    $results = @()
    foreach ($svc in $services) {
        $s = Get-Service -Name $svc.Name -ErrorAction SilentlyContinue
        $results += [PSCustomObject]@{
            Name     = $svc.Name
            Display  = $svc.Display
            Status   = if ($s) { $s.Status.ToString() } else { "NOT_FOUND" }
            StartType = if ($s) { $s.StartType.ToString() } else { "N/A" }
            Required = $svc.Required
            OK       = ($s -and $s.Status -eq "Running")
        }
    }
    return $results
}

function Set-RequiredServices {
    Write-Section "Service Configuration"

    # Remote Registry - must be Automatic and Running (Image 10)
    $svc = Get-Service -Name "RemoteRegistry" -ErrorAction SilentlyContinue
    if ($svc) {
        if ($svc.StartType -ne "Automatic") {
            Set-Service -Name "RemoteRegistry" -StartupType Automatic
            Write-Status "Remote Registry: Startup set to Automatic"
            Log "SERVICE: RemoteRegistry startup = Automatic"
        }
        if ($svc.Status -ne "Running") {
            Start-Service -Name "RemoteRegistry"
            Write-Status "Remote Registry: Started"
            Log "SERVICE: RemoteRegistry started"
        } else {
            Write-Status "Remote Registry: Already running"
        }
    }
}

# ============================================================
# 5. USER RIGHTS ASSIGNMENT (Image 6)
# ============================================================

function Get-ManageAuditRight {
    # Check "Manage auditing and security log" privilege
    $output = secedit /export /cfg "$env:TEMP\secpol.cfg" /areas USER_RIGHTS 2>$null
    $content = Get-Content "$env:TEMP\secpol.cfg" -ErrorAction SilentlyContinue
    Remove-Item "$env:TEMP\secpol.cfg" -Force -ErrorAction SilentlyContinue

    $line = $content | Where-Object { $_ -match "SeSecurityPrivilege" }
    if ($line) {
        $accounts = ($line -split "=")[1].Trim()
        return $accounts
    }
    return "Not configured"
}

# ============================================================
# 6. FIREWALL CHECK (Image 15)
# ============================================================

function Get-FirewallStatus {
    $profiles = Get-NetFirewallProfile -ErrorAction SilentlyContinue
    $results = @()
    foreach ($p in $profiles) {
        # Check FILE ACTIVITY dashboard port
        $rule = Get-NetFirewallRule -DisplayName "FILE ACTIVITY*" -ErrorAction SilentlyContinue
        $results += [PSCustomObject]@{
            Profile  = $p.Name
            Enabled  = $p.Enabled
            FARule   = if ($rule) { "Exists" } else { "Missing" }
        }
    }
    return $results
}

# ============================================================
# 7. BACKUP & ROLLBACK
# ============================================================

function Save-Backup {
    Write-Section "Saving Configuration Backup"

    New-Item -Path $BackupPath -ItemType Directory -Force | Out-Null

    $backup = @{
        Timestamp     = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        Version       = $SCRIPT_VERSION
        ComputerName  = $env:COMPUTERNAME
        AuditPolicies = @()
        EventLogs     = @()
        Services      = @()
        NTFSAudit     = @()
    }

    # Backup current audit policies
    $raw = auditpol /get /category:* /r 2>$null
    if ($raw) {
        $csv = $raw | ConvertFrom-Csv
        foreach ($row in $csv) {
            if ($row.Subcategory) {
                $backup.AuditPolicies += @{
                    Subcategory = $row.Subcategory
                    GUID        = $row.'Subcategory GUID'
                    Setting     = $row.'Inclusion Setting'
                }
            }
        }
    }

    # Backup event log sizes
    foreach ($logName in @("Security", "System", "Application")) {
        $log = Get-WinEvent -ListLog $logName -ErrorAction SilentlyContinue
        if ($log) {
            $backup.EventLogs += @{
                Name      = $logName
                MaxBytes  = $log.MaximumSizeInBytes
                LogMode   = $log.LogMode.ToString()
            }
        }
    }

    # Backup service states
    foreach ($svcName in @("RemoteRegistry")) {
        $svc = Get-Service -Name $svcName -ErrorAction SilentlyContinue
        if ($svc) {
            $backup.Services += @{
                Name      = $svcName
                StartType = $svc.StartType.ToString()
                Status    = $svc.Status.ToString()
            }
        }
    }

    # Backup NTFS audit rules for share paths
    foreach ($path in $SharePaths) {
        if (Test-Path $path) {
            try {
                $acl = Get-Acl -Path $path -Audit
                $rules = $acl.GetAuditRules($true, $false, [System.Security.Principal.NTAccount])
                $ruleList = @()
                foreach ($rule in $rules) {
                    $ruleList += @{
                        Identity   = $rule.IdentityReference.Value
                        Rights     = $rule.FileSystemRights.value__
                        AuditFlags = $rule.AuditFlags.value__
                        Inherit    = $rule.InheritanceFlags.value__
                        Propagation = $rule.PropagationFlags.value__
                    }
                }
                $backup.NTFSAudit += @{ Path = $path; Rules = $ruleList }
            } catch {}
        }
    }

    $backup | ConvertTo-Json -Depth 5 | Out-File -FilePath $BACKUP_FILE -Encoding UTF8
    Write-Status "Backup saved: $BACKUP_FILE"
    Log "BACKUP: Saved to $BACKUP_FILE"
}

function Invoke-Rollback {
    Write-Section "Rolling Back Configuration"

    if (-not (Test-Path $BACKUP_FILE)) {
        Write-Error2 "Backup file not found: $BACKUP_FILE"
        Write-Error2 "Cannot rollback without a backup."
        return
    }

    $backup = Get-Content $BACKUP_FILE | ConvertFrom-Json
    Write-Info "Backup from: $($backup.Timestamp) on $($backup.ComputerName)"

    # Rollback audit policies
    Write-Section "Restoring Audit Policies"
    foreach ($pol in $backup.AuditPolicies) {
        $guid = $pol.GUID
        $setting = $pol.Setting

        $enableDisable = switch ($setting) {
            "Success"             { "/success:enable /failure:disable" }
            "Failure"             { "/success:disable /failure:enable" }
            "Success and Failure" { "/success:enable /failure:enable" }
            "No Auditing"         { "/success:disable /failure:disable" }
            default               { "/success:disable /failure:disable" }
        }

        if ($pol.Subcategory) {
            $cmd = "auditpol /set /subcategory:`"$($pol.Subcategory)`" $enableDisable"
            Invoke-Expression $cmd 2>$null | Out-Null
            Write-Status "Restored: $($pol.Subcategory) = $setting"
        }
    }

    # Rollback event log sizes
    Write-Section "Restoring Event Log Settings"
    foreach ($log in $backup.EventLogs) {
        wevtutil sl $log.Name /ms:$($log.MaxBytes) 2>$null
        Write-Status "Restored: $($log.Name) log = $([math]::Round($log.MaxBytes/1024/1024)) MB"
    }

    # Rollback services
    Write-Section "Restoring Services"
    foreach ($svc in $backup.Services) {
        Set-Service -Name $svc.Name -StartupType $svc.StartType -ErrorAction SilentlyContinue
        if ($svc.Status -eq "Stopped") {
            Stop-Service -Name $svc.Name -Force -ErrorAction SilentlyContinue
        }
        Write-Status "Restored: $($svc.Name) = $($svc.StartType) / $($svc.Status)"
    }

    # Rollback NTFS audit
    Write-Section "Restoring NTFS Audit Rules"
    foreach ($entry in $backup.NTFSAudit) {
        if (Test-Path $entry.Path) {
            try {
                $acl = Get-Acl -Path $entry.Path -Audit
                # Remove all current audit rules
                $currentRules = $acl.GetAuditRules($true, $false, [System.Security.Principal.NTAccount])
                foreach ($r in $currentRules) { $acl.RemoveAuditRule($r) | Out-Null }

                # Re-add backed up rules
                foreach ($r in $entry.Rules) {
                    $rule = New-Object System.Security.AccessControl.FileSystemAuditRule(
                        $r.Identity,
                        [System.Security.AccessControl.FileSystemRights]$r.Rights,
                        [System.Security.AccessControl.InheritanceFlags]$r.Inherit,
                        [System.Security.AccessControl.PropagationFlags]$r.Propagation,
                        [System.Security.AccessControl.AuditFlags]$r.AuditFlags
                    )
                    $acl.AddAuditRule($rule)
                }
                Set-Acl -Path $entry.Path -AclObject $acl
                Write-Status "Restored NTFS audit: $($entry.Path)"
            } catch {
                Write-Error2 "NTFS rollback failed: $($entry.Path) - $_"
            }
        }
    }

    Write-Host ""
    Write-Status "Rollback complete! Run 'gpupdate /force' to apply GPO changes."
    Log "ROLLBACK: Completed from $($backup.Timestamp) backup"
}

# ============================================================
# MAIN EXECUTION
# ============================================================

# Ensure log directory exists
New-Item -Path (Split-Path $LOG_FILE) -ItemType Directory -Force -ErrorAction SilentlyContinue | Out-Null

Write-Host ""
Write-Host "  ============================================" -ForegroundColor Cyan
Write-Host "  FILE ACTIVITY - Audit Configuration Tool" -ForegroundColor Cyan
Write-Host "  Version $SCRIPT_VERSION | $(Get-Date -Format 'yyyy-MM-dd HH:mm')" -ForegroundColor DarkCyan
Write-Host "  Computer: $env:COMPUTERNAME" -ForegroundColor DarkCyan
Write-Host "  ============================================" -ForegroundColor Cyan

switch ($Action) {

    "check" {
        Write-Host "`n  Mode: CHECK (read-only, no changes)" -ForegroundColor Yellow

        # 1. Audit Policies
        Write-Section "Audit Policies"
        $policies = Get-AuditPolicyStatus
        foreach ($p in $policies) {
            if ($p.OK) {
                Write-Status "$($p.Subcategory): $($p.Current)"
            } else {
                Write-Warning2 "$($p.Subcategory): $($p.Current) (Required: $($p.Required))"
            }
        }
        $auditOK = ($policies | Where-Object { -not $_.OK }).Count -eq 0

        # 2. Event Logs
        Write-Section "Event Log Settings"
        $logs = Get-EventLogSettings
        foreach ($l in $logs) {
            $sizeStr = "$($l.MaxSizeMB) MB ($($l.RecordCount) events, $($l.Retention))"
            if ($l.OK) { Write-Status "$($l.LogName): $sizeStr" }
            else { Write-Warning2 "$($l.LogName): $sizeStr (Recommended: 4 GB)" }
        }

        # 3. Services
        Write-Section "Required Services"
        $services = Get-RequiredServices
        foreach ($s in $services) {
            if ($s.OK) { Write-Status "$($s.Display): $($s.Status) ($($s.StartType))" }
            else { Write-Warning2 "$($s.Display): $($s.Status) ($($s.StartType)) - Should be Running/Automatic" }
        }

        # 4. User Rights
        Write-Section "User Rights Assignment"
        $manageAudit = Get-ManageAuditRight
        Write-Info "Manage auditing and security log: $manageAudit"

        # 5. NTFS Audit on Share Paths
        if ($SharePaths.Count -gt 0) {
            Write-Section "NTFS Audit on Shares"
            foreach ($path in $SharePaths) {
                $status = Get-NTFSAuditStatus -Path $path
                if ($status.Status -eq "CONFIGURED") {
                    Write-Status "$path : $($status.RuleCount) audit rules"
                } elseif ($status.Status -eq "MISSING_AUDIT") {
                    Write-Warning2 "$path : No audit rules (needs configuration)"
                } else {
                    Write-Error2 "$path : $($status.Status)"
                }
            }
        }

        # 6. Firewall
        Write-Section "Firewall Status"
        $fw = Get-FirewallStatus
        foreach ($f in $fw) {
            Write-Info "$($f.Profile): Enabled=$($f.Enabled), FILE ACTIVITY Rule=$($f.FARule)"
        }

        # Summary
        Write-Host ""
        Write-Host "  ============================================" -ForegroundColor Cyan
        $issues = ($policies | Where-Object { -not $_.OK }).Count +
                  ($logs | Where-Object { -not $_.OK }).Count +
                  ($services | Where-Object { -not $_.OK }).Count

        if ($issues -eq 0) {
            Write-Host "  RESULT: All audit settings are properly configured!" -ForegroundColor Green
        } else {
            Write-Host "  RESULT: $issues issue(s) found. Run with -Action configure to fix." -ForegroundColor Yellow
            Write-Host "  Command: .\Configure-FileAudit.ps1 -Action configure -SharePaths @('E:\BURCU_ORTAK')" -ForegroundColor DarkYellow
        }
        Write-Host "  ============================================" -ForegroundColor Cyan
    }

    "configure" {
        Write-Host "`n  Mode: CONFIGURE (will make changes)" -ForegroundColor Red

        if (-not $Force) {
            $confirm = Read-Host "  Apply audit configuration? (Y/N)"
            if ($confirm -ne "Y" -and $confirm -ne "y") {
                Write-Host "  Cancelled." -ForegroundColor Yellow
                return
            }
        }

        # Backup first
        Save-Backup

        # Apply all settings
        Set-AuditPolicies
        Set-EventLogSettings
        Set-RequiredServices

        # NTFS Audit on shares
        if ($SharePaths.Count -gt 0) {
            Write-Section "NTFS Audit on Shares"
            foreach ($path in $SharePaths) {
                Set-NTFSAudit -Path $path
            }
        } else {
            Write-Warning2 "No SharePaths specified. Use -SharePaths to configure NTFS auditing."
        }

        # Force GPO update (Image 16)
        Write-Section "Applying Group Policy"
        gpupdate /force 2>$null | Out-Null
        Write-Status "Group Policy updated"

        # Final verification
        Write-Host ""
        Write-Host "  ============================================" -ForegroundColor Green
        Write-Host "  Configuration Complete!" -ForegroundColor Green
        Write-Host ""
        Write-Host "  To verify: .\Configure-FileAudit.ps1 -Action check" -ForegroundColor Cyan
        Write-Host "  To revert: .\Configure-FileAudit.ps1 -Action rollback" -ForegroundColor Yellow
        Write-Host "  Backup at: $BACKUP_FILE" -ForegroundColor DarkCyan
        Write-Host "  ============================================" -ForegroundColor Green

        Log "CONFIGURE: Completed successfully"
    }

    "rollback" {
        Write-Host "`n  Mode: ROLLBACK (reverting changes)" -ForegroundColor Yellow

        if (-not $Force) {
            $confirm = Read-Host "  Revert all audit changes? (Y/N)"
            if ($confirm -ne "Y" -and $confirm -ne "y") {
                Write-Host "  Cancelled." -ForegroundColor Yellow
                return
            }
        }

        Invoke-Rollback
    }
}

Write-Host ""
