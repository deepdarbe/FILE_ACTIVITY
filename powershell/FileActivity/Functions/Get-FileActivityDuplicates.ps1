function Get-FileActivityDuplicates {
    <#
    .SYNOPSIS
        Retrieve duplicate file groups for a source.

    .DESCRIPTION
        Returns groups of duplicate files (same content hash) with the wasted
        bytes per group. Wraps GET /api/reports/duplicates/{id}.

    .PARAMETER SourceId
        Numeric source ID.

    .PARAMETER Page
        1-based page index. Default 1.

    .PARAMETER PageSize
        Rows per page (1-500). Default 50.

    .PARAMETER MinSize
        Minimum file size in bytes to include. Default 0 (no filter).

    .EXAMPLE
        Get-FileActivityDuplicates -SourceId 1 |
            Where-Object FileSize -gt 1GB |
            Sort-Object WasteSize -Descending |
            Select-Object -First 10

    .OUTPUTS
        PSCustomObject (one per duplicate group) with: ContentHash, FileSize,
        FileSizeFormatted, Count, WasteSize, WasteSizeFormatted, Files
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][int]$SourceId,
        [int]$Page = 1,
        [int]$PageSize = 50,
        [long]$MinSize = 0
    )
    $url = "$script:BaseUrl/api/reports/duplicates/$SourceId" +
           "?page=$Page&page_size=$PageSize&min_size=$MinSize"
    try {
        $r = Invoke-RestMethod -Uri $url -Method GET -ErrorAction Stop
        foreach ($g in $r.groups) {
            [PSCustomObject]@{
                ContentHash        = $g.content_hash
                FileSize           = $g.file_size
                FileSizeFormatted  = $g.file_size_formatted
                Count              = $g.count
                WasteSize          = $g.waste_size
                WasteSizeFormatted = $g.waste_size_formatted
                Files              = $g.files
            }
        }
    } catch {
        Write-Error "Get-FileActivityDuplicates failed: $_"
    }
}
