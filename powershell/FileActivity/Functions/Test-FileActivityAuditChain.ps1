function Test-FileActivityAuditChain {
    <#
    .SYNOPSIS
        Verify the integrity of the tamper-evident audit chain.

    .DESCRIPTION
        Calls GET /api/audit/verify and projects the result into a flat
        PSCustomObject so callers can use:
            if ((Test-FileActivityAuditChain).Verified) { ... }

    .PARAMETER SinceSeq
        Verify the chain from this sequence number forward. Default 1.

    .PARAMETER EndSeq
        Optional upper bound sequence number.

    .EXAMPLE
        $r = Test-FileActivityAuditChain
        if (-not $r.Verified) { Write-Warning "Chain broken at seq $($r.BrokenAtSeq)" }

    .OUTPUTS
        PSCustomObject with: Verified (bool), BrokenAtSeq (int|null),
        BrokenReason (string|null), Total (int)
    #>
    [CmdletBinding()]
    param(
        [int]$SinceSeq = 1,
        [int]$EndSeq
    )
    $url = "$script:BaseUrl/api/audit/verify?since_seq=$SinceSeq"
    if ($PSBoundParameters.ContainsKey('EndSeq')) {
        $url += "&end_seq=$EndSeq"
    }
    try {
        $r = Invoke-RestMethod -Uri $url -Method GET -ErrorAction Stop
        [PSCustomObject]@{
            Verified     = [bool]$r.verified
            BrokenAtSeq  = $r.broken_at
            BrokenReason = $r.broken_reason
            Total        = $r.total
        }
    } catch {
        Write-Error "Test-FileActivityAuditChain failed: $_"
    }
}
