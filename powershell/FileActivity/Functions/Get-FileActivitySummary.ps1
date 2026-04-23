function Get-FileActivitySummary {
    <#
    .SYNOPSIS
        Retrieve the latest scan summary (KPIs) for a source.

    .DESCRIPTION
        Returns the precomputed Instant Overview KPIs for the most recent
        completed scan. Wraps GET /api/overview/{id}.

    .PARAMETER SourceId
        Numeric source ID.

    .EXAMPLE
        Get-FileActivitySummary -SourceId 1

    .OUTPUTS
        PSCustomObject with: SourceId, ScanId, HasData, TotalFiles, TotalSize,
        TotalSizeFormatted, StaleSize, LargeSize, DuplicateWasteSize,
        TopExtensions, TopOwners
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][int]$SourceId
    )
    $url = "$script:BaseUrl/api/overview/$SourceId"
    try {
        $r = Invoke-RestMethod -Uri $url -Method GET -ErrorAction Stop
        [PSCustomObject]@{
            SourceId             = $SourceId
            ScanId               = $r.scan_id
            HasData              = [bool]$r.has_data
            TotalFiles           = $r.total_files
            TotalSize            = $r.total_size
            TotalSizeFormatted   = $r.total_size_formatted
            StaleSize            = $r.stale_size
            StaleSizeFormatted   = $r.stale_size_formatted
            LargeSize            = $r.large_size
            LargeSizeFormatted   = $r.large_size_formatted
            DuplicateWasteSize   = $r.duplicate_waste_size
            DuplicateWasteFormatted = $r.duplicate_waste_formatted
            TopExtensions        = $r.top_extensions
            TopOwners            = $r.top_owners
        }
    } catch {
        Write-Error "Get-FileActivitySummary failed: $_"
    }
}
