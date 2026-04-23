# FileActivity PowerShell module
# Wraps the FILE ACTIVITY dashboard REST API (default http://localhost:8085)
# so that pipeline-friendly PSCustomObject results can be composed naturally.

# Module-scoped base URL with environment variable fallback.
$script:BaseUrl = if ($env:FILEACTIVITY_BASE_URL) {
    $env:FILEACTIVITY_BASE_URL.TrimEnd('/')
} else {
    'http://localhost:8085'
}

# Dot-source every Functions/*.ps1 so each cmdlet lives in its own file.
$functionFolder = Join-Path $PSScriptRoot 'Functions'
if (Test-Path $functionFolder) {
    Get-ChildItem -Path $functionFolder -Filter '*.ps1' | ForEach-Object {
        . $_.FullName
    }
}

function Set-FileActivityBaseUrl {
    <#
    .SYNOPSIS
        Override the dashboard base URL used by FileActivity cmdlets.

    .DESCRIPTION
        Useful when targeting a remote FILE ACTIVITY dashboard or a non-default
        port. The new value persists for the lifetime of the PowerShell session.

    .PARAMETER Url
        Full URL including scheme and port (e.g. http://other-host:8085).

    .EXAMPLE
        Set-FileActivityBaseUrl 'http://files01.lan:8085'
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Url
    )
    $script:BaseUrl = $Url.TrimEnd('/')
}

function Get-FileActivityBaseUrl {
    <#
    .SYNOPSIS
        Return the dashboard base URL currently used by FileActivity cmdlets.

    .EXAMPLE
        Get-FileActivityBaseUrl
    #>
    [CmdletBinding()]
    param()
    return $script:BaseUrl
}

Export-ModuleMember -Function * -Variable @() -Cmdlet @()
