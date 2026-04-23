function Get-FileActivityAudit {
    <#
    .SYNOPSIS
        Retrieve audit events from the tamper-evident audit log.

    .DESCRIPTION
        Returns audit events (file operations, archive ops, scans, ransomware
        actions) over the requested window. Wraps GET /api/audit/events.

    .PARAMETER SourceId
        Optional source filter.

    .PARAMETER EventType
        Optional event type filter (e.g. archive, restore, scan, alert).

    .PARAMETER Username
        Optional user filter.

    .PARAMETER Days
        Lookback window in days (1-365). Default 7.

    .PARAMETER Page
        Page index (1-10000). Default 1.

    .EXAMPLE
        Get-FileActivityAudit -EventType archive -Days 30

    .OUTPUTS
        PSCustomObject with: Id, Seq, Timestamp, EventType, SourceId, Username,
        FilePath, Details
    #>
    [CmdletBinding()]
    param(
        [int]$SourceId,
        [string]$EventType,
        [string]$Username,
        [int]$Days = 7,
        [int]$Page = 1
    )
    $params = @("days=$Days", "page=$Page")
    if ($PSBoundParameters.ContainsKey('SourceId')) { $params += "source_id=$SourceId" }
    if ($EventType) { $params += "event_type=$([uri]::EscapeDataString($EventType))" }
    if ($Username)  { $params += "username=$([uri]::EscapeDataString($Username))" }
    $url = "$script:BaseUrl/api/audit/events?" + ($params -join '&')
    try {
        $r = Invoke-RestMethod -Uri $url -Method GET -ErrorAction Stop
        # API returns either a list or a paged object {events, total, page,...}
        $events = if ($r -is [System.Collections.IEnumerable] -and -not ($r -is [string])) {
            $r
        } elseif ($r.events) {
            $r.events
        } else {
            @($r)
        }
        foreach ($e in $events) {
            [PSCustomObject]@{
                Id        = $e.id
                Seq       = $e.seq
                Timestamp = $e.timestamp
                EventType = $e.event_type
                SourceId  = $e.source_id
                Username  = $e.username
                FilePath  = $e.file_path
                Details   = $e.details
            }
        }
    } catch {
        Write-Error "Get-FileActivityAudit failed: $_"
    }
}
