function Get-FileActivityServiceStatus {
    <#
    .SYNOPSIS
        Show current state and start type of the FILE ACTIVITY Windows service.

    .DESCRIPTION
        Returns Status (Running/Stopped/...) and StartType (Automatic/Manual/
        Disabled) of the NSSM-managed "FileActivity" service installed by
        deploy\install_service.ps1 (issue #151). Read-only; non-admin
        sessions can call it.

        If the service is not installed, returns a PSCustomObject with
        Status='NotInstalled' rather than throwing — friendlier for
        scripts that branch on install state.

    .PARAMETER ServiceName
        Override the service name. Default "FileActivity".

    .EXAMPLE
        Get-FileActivityServiceStatus

    .EXAMPLE
        if ((Get-FileActivityServiceStatus).Status -ne 'Running') { Start-FileActivityService }

    .OUTPUTS
        PSCustomObject with Name, Status, StartType, DisplayName
    #>
    [CmdletBinding()]
    param(
        [string]$ServiceName = 'FileActivity'
    )
    try {
        $svc = Get-Service -Name $ServiceName -ErrorAction Stop
        [PSCustomObject]@{
            Name        = $svc.Name
            Status      = $svc.Status
            StartType   = $svc.StartType
            DisplayName = $svc.DisplayName
        }
    } catch [Microsoft.PowerShell.Commands.ServiceCommandException] {
        [PSCustomObject]@{
            Name        = $ServiceName
            Status      = 'NotInstalled'
            StartType   = $null
            DisplayName = $null
        }
    } catch {
        Write-Error "Get-FileActivityServiceStatus failed: $_"
    }
}
