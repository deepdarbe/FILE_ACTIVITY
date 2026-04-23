function Get-FileActivityRansomware {
    <#
    .SYNOPSIS
        Retrieve recent ransomware detection alerts.

    .DESCRIPTION
        Returns ransomware alerts raised in the last N minutes. Wraps
        GET /api/security/ransomware/alerts.

    .PARAMETER SinceMinutes
        Lookback window in minutes (1-10080). Default 60.

    .EXAMPLE
        Get-FileActivityRansomware | Where-Object Severity -eq 'critical'

    .OUTPUTS
        PSCustomObject with: Id, SourceId, RuleName, Severity, Username,
        DetectedAt, Details, AcknowledgedAt, AcknowledgedBy
    #>
    [CmdletBinding()]
    param(
        [int]$SinceMinutes = 60
    )
    $url = "$script:BaseUrl/api/security/ransomware/alerts?since_minutes=$SinceMinutes"
    try {
        $response = Invoke-RestMethod -Uri $url -Method GET -ErrorAction Stop
        $response | ForEach-Object {
            [PSCustomObject]@{
                Id              = $_.id
                SourceId        = $_.source_id
                RuleName        = $_.rule_name
                Severity        = $_.severity
                Username        = $_.username
                DetectedAt      = $_.detected_at
                Details         = $_.details
                AcknowledgedAt  = $_.acknowledged_at
                AcknowledgedBy  = $_.acknowledged_by
            }
        }
    } catch {
        Write-Error "Get-FileActivityRansomware failed: $_"
    }
}
