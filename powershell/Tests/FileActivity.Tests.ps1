Describe 'FileActivity Module' {
    BeforeAll {
        $modulePath = Join-Path $PSScriptRoot '..' 'FileActivity' 'FileActivity.psd1'
        Import-Module $modulePath -Force
    }

    AfterAll {
        Remove-Module FileActivity -Force -ErrorAction SilentlyContinue
    }

    It 'Imports cleanly' {
        Get-Module FileActivity | Should -Not -BeNullOrEmpty
    }

    It 'Exports expected cmdlets' {
        $exports = (Get-Module FileActivity).ExportedFunctions.Keys
        $expected = @(
            'Get-FileActivityScan',
            'Start-FileActivityScan',
            'Get-FileActivitySummary',
            'Get-FileActivityDuplicates',
            'Get-FileActivityRansomware',
            'Invoke-FileActivityArchive',
            'Get-FileActivityAudit',
            'Test-FileActivityAuditChain'
        )
        foreach ($cmd in $expected) {
            $exports | Should -Contain $cmd
        }
    }

    It 'Exposes Set-FileActivityBaseUrl and Get-FileActivityBaseUrl' {
        $exports = (Get-Module FileActivity).ExportedFunctions.Keys
        $exports | Should -Contain 'Set-FileActivityBaseUrl'
        $exports | Should -Contain 'Get-FileActivityBaseUrl'
    }

    It 'Defaults base URL to localhost:8085 when env var unset' {
        # Re-import with env var cleared to assert the documented default.
        $previous = $env:FILEACTIVITY_BASE_URL
        try {
            Remove-Item Env:FILEACTIVITY_BASE_URL -ErrorAction SilentlyContinue
            $modulePath = Join-Path $PSScriptRoot '..' 'FileActivity' 'FileActivity.psd1'
            Import-Module $modulePath -Force
            (Get-FileActivityBaseUrl) | Should -Be 'http://localhost:8085'
        } finally {
            if ($null -ne $previous) { $env:FILEACTIVITY_BASE_URL = $previous }
        }
    }

    It 'Set-FileActivityBaseUrl updates the module-scoped URL' {
        Set-FileActivityBaseUrl 'http://example.com:1234'
        (Get-FileActivityBaseUrl) | Should -Be 'http://example.com:1234'
    }

    It 'Set-FileActivityBaseUrl trims trailing slashes' {
        Set-FileActivityBaseUrl 'http://example.com:1234/'
        (Get-FileActivityBaseUrl) | Should -Be 'http://example.com:1234'
    }

    It 'Every cmdlet ships comment-based help' {
        $cmds = @(
            'Get-FileActivityScan', 'Start-FileActivityScan',
            'Get-FileActivitySummary', 'Get-FileActivityDuplicates',
            'Get-FileActivityRansomware', 'Invoke-FileActivityArchive',
            'Get-FileActivityAudit', 'Test-FileActivityAuditChain'
        )
        foreach ($cmd in $cmds) {
            $help = Get-Help $cmd -ErrorAction SilentlyContinue
            $help.Synopsis | Should -Not -BeNullOrEmpty
        }
    }
}
