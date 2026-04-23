function Invoke-FileActivityArchive {
    <#
    .SYNOPSIS
        Trigger an archive run for a source.

    .DESCRIPTION
        Posts to /api/archive/run to start the copy-verify-delete archive
        workflow for the supplied source. Use -DryRun to preview without
        moving anything (calls /api/archive/dry-run instead).

    .PARAMETER SourceId
        Numeric source ID to archive.

    .PARAMETER DryRun
        Switch - when present, the call is sent to /api/archive/dry-run so
        no files are actually moved.

    .EXAMPLE
        Invoke-FileActivityArchive -SourceId 1 -DryRun

    .OUTPUTS
        PSCustomObject with: SourceId, OperationId, FilesArchived, BytesArchived,
        Status, DryRun
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][int]$SourceId,
        [switch]$DryRun
    )
    $endpoint = if ($DryRun) { 'dry-run' } else { 'run' }
    $url = "$script:BaseUrl/api/archive/$endpoint"
    $body = @{ source_id = $SourceId } | ConvertTo-Json
    $action = if ($DryRun) { 'Dry-run archive' } else { 'Archive' }
    if (-not $PSCmdlet.ShouldProcess("source $SourceId", $action)) {
        return
    }
    try {
        $r = Invoke-RestMethod -Uri $url -Method POST -Body $body `
                               -ContentType 'application/json' -ErrorAction Stop
        [PSCustomObject]@{
            SourceId       = $SourceId
            OperationId    = $r.operation_id
            FilesArchived  = $r.files_archived
            BytesArchived  = $r.bytes_archived
            Status         = $r.status
            DryRun         = [bool]$DryRun
        }
    } catch {
        Write-Error "Invoke-FileActivityArchive failed: $_"
    }
}
