function Start-FileActivityScan {
    <#
    .SYNOPSIS
        Start a new scan for a source.

    .DESCRIPTION
        Triggers an asynchronous scan on the given source by POSTing to
        /api/scan/{id}. The dashboard runs the scan in a background thread;
        poll Get-FileActivityScan to follow progress.

    .PARAMETER SourceId
        Numeric source ID to scan.

    .EXAMPLE
        Start-FileActivityScan -SourceId 1

    .OUTPUTS
        PSCustomObject with: SourceId, Status, Message
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][int]$SourceId
    )
    $url = "$script:BaseUrl/api/scan/$SourceId"
    if (-not $PSCmdlet.ShouldProcess("source $SourceId", 'Start scan')) {
        return
    }
    try {
        $response = Invoke-RestMethod -Uri $url -Method POST -ErrorAction Stop
        [PSCustomObject]@{
            SourceId = $SourceId
            Status   = $response.status
            Message  = $response.message
        }
    } catch {
        Write-Error "Start-FileActivityScan failed: $_"
    }
}
