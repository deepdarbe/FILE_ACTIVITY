function Stop-FileActivityService {
    <#
    .SYNOPSIS
        Stop the FILE ACTIVITY Windows service.

    .DESCRIPTION
        Wraps Stop-Service for the NSSM-managed "FileActivity" service
        (issue #151). Requires elevation; non-admin sessions surface a
        friendly error. Uses -Force to drop dependent services if any
        appear in the future.

    .PARAMETER ServiceName
        Override the service name. Default "FileActivity".

    .EXAMPLE
        Stop-FileActivityService

    .OUTPUTS
        Microsoft.PowerShell.Commands.ServiceController
    #>
    [CmdletBinding()]
    param(
        [string]$ServiceName = 'FileActivity'
    )
    try {
        $svc = Get-Service -Name $ServiceName -ErrorAction Stop
        Stop-Service -InputObject $svc -Force -ErrorAction Stop
        Get-Service -Name $ServiceName
    } catch [System.InvalidOperationException] {
        if ($_.Exception.Message -match 'access is denied|Access is denied') {
            Write-Error "Stop-FileActivityService: Yonetici hakki gerekli."
        } else {
            Write-Error "Stop-FileActivityService failed: $_"
        }
    } catch {
        Write-Error "Stop-FileActivityService failed: $_"
    }
}
