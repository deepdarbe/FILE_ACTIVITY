function Get-FileActivityScan {
    <#
    .SYNOPSIS
        Retrieve scan history for a source.

    .DESCRIPTION
        Returns scan_run records for the given source ID, newest first.
        Wraps GET /api/sources/{id}/scans.

    .PARAMETER SourceId
        Numeric source ID. Use the sources list from the dashboard to discover IDs.

    .PARAMETER Limit
        Max number of scans to return. Default 20.

    .EXAMPLE
        Get-FileActivityScan -SourceId 1 | Where-Object Status -eq 'completed'

    .OUTPUTS
        PSCustomObject with: Id, SourceId, StartedAt, CompletedAt, TotalFiles,
        TotalSize, Errors, Status
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][int]$SourceId,
        [int]$Limit = 20
    )
    $url = "$script:BaseUrl/api/sources/$SourceId/scans?limit=$Limit"
    try {
        $response = Invoke-RestMethod -Uri $url -Method GET -ErrorAction Stop
        $response | ForEach-Object {
            [PSCustomObject]@{
                Id          = $_.id
                SourceId    = $_.source_id
                StartedAt   = $_.started_at
                CompletedAt = $_.completed_at
                TotalFiles  = $_.total_files
                TotalSize   = $_.total_size
                Errors      = $_.errors
                Status      = $_.status
            }
        }
    } catch {
        Write-Error "Get-FileActivityScan failed: $_"
    }
}
