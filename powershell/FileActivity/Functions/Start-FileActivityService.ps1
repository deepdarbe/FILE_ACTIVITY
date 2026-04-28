function Start-FileActivityService {
    <#
    .SYNOPSIS
        Start the FILE ACTIVITY Windows service.

    .DESCRIPTION
        Wraps Start-Service for the NSSM-managed "FileActivity" service
        installed via deploy\install_service.ps1 (issue #151). Requires the
        current PowerShell session to run elevated; otherwise Windows
        rejects the service control request and we surface a friendly
        error.

    .PARAMETER ServiceName
        Override the service name. Default "FileActivity" matches what
        install_service.ps1 creates.

    .EXAMPLE
        Start-FileActivityService

    .EXAMPLE
        Start-FileActivityService -ServiceName 'FileActivity-Dev'

    .OUTPUTS
        Microsoft.PowerShell.Commands.ServiceController
    #>
    [CmdletBinding()]
    param(
        [string]$ServiceName = 'FileActivity'
    )
    try {
        $svc = Get-Service -Name $ServiceName -ErrorAction Stop
        Start-Service -InputObject $svc -ErrorAction Stop
        # Re-fetch so caller sees the updated Status
        Get-Service -Name $ServiceName
    } catch [System.InvalidOperationException] {
        if ($_.Exception.Message -match 'access is denied|Access is denied') {
            Write-Error "Start-FileActivityService: Yonetici hakki gerekli. PowerShell'i 'Yonetici olarak calistir' ile yeniden acin."
        } else {
            Write-Error "Start-FileActivityService failed: $_"
        }
    } catch {
        Write-Error "Start-FileActivityService failed: $_"
    }
}
