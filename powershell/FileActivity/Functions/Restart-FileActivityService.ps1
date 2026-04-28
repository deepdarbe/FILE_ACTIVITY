function Restart-FileActivityService {
    <#
    .SYNOPSIS
        Restart the FILE ACTIVITY Windows service.

    .DESCRIPTION
        Wraps Restart-Service for the NSSM-managed "FileActivity" service
        (issue #151). Useful after a config change or update.cmd run when
        you want to bounce the dashboard without dropping to Services.msc.

    .PARAMETER ServiceName
        Override the service name. Default "FileActivity".

    .EXAMPLE
        Restart-FileActivityService

    .OUTPUTS
        Microsoft.PowerShell.Commands.ServiceController
    #>
    [CmdletBinding()]
    param(
        [string]$ServiceName = 'FileActivity'
    )
    try {
        $svc = Get-Service -Name $ServiceName -ErrorAction Stop
        Restart-Service -InputObject $svc -Force -ErrorAction Stop
        Get-Service -Name $ServiceName
    } catch [System.InvalidOperationException] {
        if ($_.Exception.Message -match 'access is denied|Access is denied') {
            Write-Error "Restart-FileActivityService: Yonetici hakki gerekli."
        } else {
            Write-Error "Restart-FileActivityService failed: $_"
        }
    } catch {
        Write-Error "Restart-FileActivityService failed: $_"
    }
}
